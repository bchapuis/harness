//! The client view (harness spec §7.4): [`Harness`], [`SessionRef`], and the
//! ephemeral reply-to actor behind the blocking `prompt` convenience.
//!
//! A session is a grain, so addressing is granary's: [`SessionRef`] wraps a
//! [`GrainRef<Agent>`](granary::GrainRef) and `ask`s it the §7.3 commands,
//! location-transparently (granary §4.3). [`Harness::new`] hosts one `Agent`
//! grain per kind via `granary_named` (each `KindId` is its own grain type,
//! §2.2), injecting the node's model and sandbox seams into every activation
//! through the factory.
//!
//! [`HarnessSystem`] is the one small thing the harness needs over
//! [`GranarySystem`]: a way to emit its §10.4 events onto the same observability
//! stream the grain's events ride. Everything else the agent needs — virtual
//! time, task launching, placement, the journal — is granary's.

use std::collections::BTreeMap;
use std::sync::Arc;
use std::sync::OnceLock;
use std::time::Duration;

use actor_cluster::ClusterSystem;
use actor_cluster::Transport;
use actor_core::Actor;
use actor_core::CallError;
use actor_core::Clock;
use actor_core::Ctx;
use actor_core::Entropy;
use actor_core::Event;
use actor_core::Handler;
use actor_core::HandlerRegistry;
use actor_core::LocalSystem;
use actor_core::Spawner;
use futures::channel::oneshot;
use granary::Granary;
use granary::GrainError;
use granary::GrainRef;
use granary::GranaryExt;
use granary::GranarySystem;

use crate::agent::Agent;
use crate::agent::AttachOutcome;
use crate::agent::Cancel;
use crate::agent::RunCompleted;
use crate::agent::Tail;
use crate::agent::submit_and_attach;
use crate::kind::Kinds;
use crate::model::Model;
use crate::sandbox::SandboxProvider;
use crate::session::KindId;
use crate::session::Lineage;
use crate::session::Record;
use crate::session::RunOutcome;
use crate::session::SessionId;
use crate::session::Turn;
use crate::session::TurnId;
use granary::Seq;
use granary::Subscription;

/// What the harness needs from the actor system beyond [`GranarySystem`]: a way
/// to emit its observability events (§10.4) onto the framework's stream — the
/// same stream the grain's events ride, so checkers see one ordered sequence.
/// One trait, two implementations, so the harness runs unchanged on a single
/// node, a cluster, or the simulator.
pub trait HarnessSystem: GranarySystem {
    /// Emit a harness event onto the observability stream (§10.4).
    fn emit_app(&self, event: Event);
}

impl<C: Clock, E: Entropy, S: Spawner> HarnessSystem for LocalSystem<C, E, S> {
    fn emit_app(&self, event: Event) {
        self.emit(event);
    }
}

impl<C: Clock, E: Entropy, S: Spawner, T: Transport> HarnessSystem for ClusterSystem<C, E, S, T> {
    fn emit_app(&self, event: Event) {
        self.emit(event);
    }
}

/// Harness tuning (harness spec §7.2, §9.1): the few knobs the spec calls
/// configurable. Idle/snapshot/shard policy is per-kind `GranaryConfig` (§7.1),
/// not here; durability retries are the grain's.
#[derive(Clone, Debug)]
pub struct HarnessConfig {
    /// Default deadline bounding a caller's wait on `prompt` (§7.3) — never the
    /// run, which continues unaffected when the caller times out.
    pub submit_deadline: Duration,
    /// Default per-tool execution bound (§5.3 item 3), overridable per
    /// declaration. SHOULD default to about 5 minutes. Timed against the virtual
    /// `Clock`, so the bound is deterministic under simulation.
    pub tool_timeout: Duration,
    /// The token floor below which no model call is issued (§9.1 item 2): a
    /// near-zero `max_tokens` call still pays its full input.
    pub budget_floor: u64,
}

impl Default for HarnessConfig {
    fn default() -> Self {
        HarnessConfig {
            submit_deadline: Duration::from_secs(30),
            tool_timeout: Duration::from_secs(300),
            budget_floor: 0,
        }
    }
}

/// The node-local seams and handles every activation shares (§7.4). Injected
/// into each `Agent` activation by the [`Harness::new`] factory.
pub struct Shared<S: HarnessSystem> {
    pub(crate) kinds: Kinds,
    pub(crate) model: Arc<dyn Model>,
    pub(crate) sandboxes: Arc<dyn SandboxProvider>,
    pub(crate) config: HarnessConfig,
    /// One `Granary` handle per kind, set once after all kinds are hosted, so a
    /// grain can address children of any kind for delegation (§8.1) and cancel
    /// propagation (§9.2). Filled after construction (the factories that produce
    /// activations are themselves captured before the handles exist).
    pub(crate) granaries: OnceLock<BTreeMap<KindId, Granary<Agent<S>>>>,
}

impl<S: HarnessSystem> Shared<S> {
    /// The per-kind `Granary` handles (set in [`Harness::new`]).
    pub(crate) fn granaries(&self) -> &BTreeMap<KindId, Granary<Agent<S>>> {
        self.granaries.get().expect("granaries set in Harness::new")
    }
}

/// One node's harness (harness spec §7.4): hosts every kind's grain type and
/// injects the model and sandbox seams (§4, §5.3). Cheap to clone; every clone
/// shares the node's granaries and seams.
pub struct Harness<S: HarnessSystem> {
    shared: Arc<Shared<S>>,
    system: S,
}

impl<S: HarnessSystem> Clone for Harness<S> {
    fn clone(&self) -> Self {
        Harness {
            shared: Arc::clone(&self.shared),
            system: self.system.clone(),
        }
    }
}

impl<S: HarnessSystem> Harness<S> {
    /// Stand up the harness on this node: host one `Agent` grain per kind, each
    /// under the kind's name with the kind's `GranaryConfig` (§2.2, §7.1). Call
    /// once per node, with the same `kinds` everywhere.
    pub fn new(
        system: S,
        kinds: Kinds,
        model: Arc<dyn Model>,
        sandboxes: Arc<dyn SandboxProvider>,
    ) -> Harness<S> {
        Harness::with_config(system, kinds, model, sandboxes, HarnessConfig::default())
    }

    /// [`Harness::new`] with explicit tuning.
    pub fn with_config(
        system: S,
        kinds: Kinds,
        model: Arc<dyn Model>,
        sandboxes: Arc<dyn SandboxProvider>,
        config: HarnessConfig,
    ) -> Harness<S> {
        let shared = Arc::new(Shared {
            kinds: kinds.clone(),
            model,
            sandboxes,
            config,
            granaries: OnceLock::new(),
        });
        // Host one grain type per kind. The factory captures this node's seams
        // (via `shared`) and builds a fresh activation per (re)activation, so the
        // grain needs no `Default` and no process-global — multi-node-in-one-
        // process simulations each get their own (§12.1).
        let mut granaries = BTreeMap::new();
        for (kind_id, kind) in kinds.iter() {
            // A grain type name must be `&'static` (the `GrainName` tag, §5.1);
            // kinds are a bounded deployment-time set, so leaking is sound.
            let grain_type: &'static str = Box::leak(kind_id.as_str().to_string().into_boxed_str());
            let factory_shared = Arc::clone(&shared);
            // Capture this kind's definition so the activation resolves its kind
            // without a lookup — one grain type per kind, so it is always this one.
            let factory_kind = Arc::clone(kind);
            let granary = system.granary_named::<Agent<S>>(
                grain_type,
                kind.config.clone(),
                Arc::new(move || Agent::new(Arc::clone(&factory_shared), Arc::clone(&factory_kind))),
            );
            granaries.insert(kind_id.clone(), granary);
        }
        shared
            .granaries
            .set(granaries)
            .unwrap_or_else(|_| panic!("granaries set once"));
        Harness { shared, system }
    }

    /// A client view of `session` under `kind` (harness spec §7.4). Pure:
    /// name→shard is a local hash, no I/O. Creation is implicit in the first turn.
    pub fn session(&self, kind: &str, session: SessionId) -> SessionRef<S> {
        let kind = KindId::new(kind);
        let grain = self
            .shared
            .granaries()
            .get(&kind)
            .expect("kind registered on this node")
            .grain(session.as_str());
        SessionRef {
            grain,
            kind,
            session,
            system: self.system.clone(),
            config: self.shared.config.clone(),
        }
    }

    /// The actor system this harness runs on.
    pub fn system(&self) -> &S {
        &self.system
    }
}

/// A typed client handle to one session (harness spec §7.4): a thin agent-facing
/// surface over [`GrainRef<Agent>`](granary::GrainRef).
pub struct SessionRef<S: HarnessSystem> {
    grain: GrainRef<Agent<S>>,
    kind: KindId,
    session: SessionId,
    system: S,
    config: HarnessConfig,
}

impl<S: HarnessSystem> Clone for SessionRef<S> {
    fn clone(&self) -> Self {
        SessionRef {
            grain: self.grain.clone(),
            kind: self.kind.clone(),
            session: self.session.clone(),
            system: self.system.clone(),
            config: self.config.clone(),
        }
    }
}

impl<S: HarnessSystem> SessionRef<S> {
    /// The session's durable identity.
    pub fn id(&self) -> &SessionId {
        &self.session
    }

    /// Submit a turn and await its run's terminal outcome (harness spec §7.3,
    /// §7.4): the blocking convenience composed at the edge — `Submit` (ack) plus
    /// awaiting the one `RunCompleted` notification. The deadline bounds this
    /// caller's **wait**, never the run: on a lapse it re-submits the same
    /// `TurnId`, which re-attaches or returns the recorded outcome (H7).
    pub async fn prompt(&self, turn: Turn) -> Result<RunOutcome, GrainError> {
        self.submit(turn, None, self.config.submit_deadline).await
    }

    /// [`prompt`](Self::prompt) with an explicit wait deadline.
    pub async fn prompt_within(&self, turn: Turn, within: Duration) -> Result<RunOutcome, GrainError> {
        self.submit(turn, None, within).await
    }

    /// The submission protocol behind `prompt` and delegation (§8.1): `Submit`
    /// the turn carrying an ephemeral reply-to mailbox, await the one
    /// `RunCompleted`, and on a wait lapse re-submit the same `TurnId` to
    /// re-attach. A failed `Submit` is **not** transparently retried (granary
    /// §2.2): an ambiguous transport failure surfaces as `GrainError`.
    pub(crate) async fn submit(
        &self,
        turn: Turn,
        parent: Option<Lineage>,
        within: Duration,
    ) -> Result<RunOutcome, GrainError> {
        let started = self.system.now();
        loop {
            let elapsed = self.system.now().duration_since(started);
            let Some(remaining) = within.checked_sub(elapsed).filter(|d| !d.is_zero()) else {
                return Err(GrainError::Call(CallError::Timeout));
            };
            match submit_and_attach(
                &self.system,
                &self.grain,
                &self.kind,
                &turn,
                parent.as_ref(),
                remaining,
            )
            .await
            {
                AttachOutcome::Completed(outcome) => return Ok(outcome),
                // Mailbox dropped or wait lapsed: re-submit the same TurnId.
                AttachOutcome::Lapsed => continue,
                // A permanent caller-contract violation, surfaced as a system
                // failure; not transparently retried (granary §2.2).
                AttachOutcome::Rejected(reject) => {
                    return Err(GrainError::Call(CallError::System(reject.to_string())));
                }
                // An ambiguous transport failure surfaces as `GrainError`.
                AttachOutcome::Unreachable(e) => return Err(e),
            }
        }
    }

    /// Cancel the run `turn` names (harness spec §7.3, §9.2): idempotent.
    pub async fn cancel(&self, turn: &TurnId) -> Result<(), GrainError> {
        self.grain.ask(Cancel { turn: turn.clone() }).await
    }

    /// Read committed records (harness spec §10.2): at most `limit` records after
    /// `from`. An idempotent, replication-free read served from the activation.
    pub async fn tail(&self, from: Seq, limit: u32) -> Result<Vec<(Seq, Record)>, GrainError> {
        self.grain.ask(Tail { from, limit }).await
    }

    /// Follow the session's records live from `from` (granary §7.9): the
    /// push-based replacement for poll-tailing. The returned [`Follower`] rides a
    /// grain record subscription and reconciles by `Seq`, so it yields the exact
    /// committed sequence in order — backfilling from the journal across a leader
    /// move or hibernation (granary G16). Caller-driven: pull batches with
    /// [`Follower::next`].
    pub fn follow(&self, from: Seq) -> Follower<S> {
        Follower {
            grain: self.grain.clone(),
            system: self.system.clone(),
            sub: None,
            last: from,
        }
    }
}

/// Journal page size for a follower's backfill reads.
const FOLLOW_PAGE: u32 = 256;

/// How long a caught-up follower waits for a live record before re-checking the
/// journal. A silent leader move or crash leaves the old sink alive but idle —
/// the stream never closes and the new leader has no sink — so a periodic
/// backfill is the liveness net that detects the move and re-subscribes (§7.9).
/// During active streaming records arrive by push well within this, so it is a
/// safety net, not the steady-state path.
const FOLLOW_RESYNC: Duration = Duration::from_secs(2);

/// A live follower over a session's journal (granary §7.9), the push-based
/// replacement for poll-tailing. It rides a grain record subscription and
/// reconciles by `Seq`: it backfills from the journal on first attach, on any
/// gap, and after the stream closes (a leader move, a lag-drop, or hibernation),
/// so [`next`](Self::next) yields the exact committed sequence — in order, with
/// no gap or duplicate (granary G16). Push is the fast path; the journal is the
/// authority.
pub struct Follower<S: HarnessSystem> {
    grain: GrainRef<Agent<S>>,
    /// For the re-sync liveness timer (a silent move leaves the stream open).
    system: S,
    /// The live subscription, or `None` before the first attach / after a close.
    sub: Option<Subscription<Agent<S>>>,
    /// The highest seq handed to the caller: the reconciliation cursor.
    last: Seq,
}

impl<S: HarnessSystem> Follower<S> {
    /// The next batch of in-order records after the last one returned, with at
    /// least one record. Blocks until records are available, attaching,
    /// backfilling, and re-subscribing transparently. A `GrainError` is a real
    /// durability outcome (the shard cannot serve right now) the caller may
    /// surface and retry.
    pub async fn next(&mut self) -> Result<Vec<(Seq, Record)>, GrainError> {
        loop {
            // (Re)attach if needed: subscribe registers a sink and returns the
            // head, so any commit from here on is pushed; the backfill below
            // closes whatever gap preceded the attach (a late start or a move).
            if self.sub.is_none() {
                self.sub = Some(self.grain.subscribe(self.last).await?);
            }
            // Backfill straight from the journal (the source of truth) until the
            // cursor reaches the head; return as soon as a page has records.
            if let Some(batch) = self.backfill().await? {
                return Ok(batch);
            }
            // Caught up: race the next live batch against a re-sync timer. Clone
            // the receiver so no borrow of `self.sub` is held across the await.
            let rx = self.sub.as_ref().expect("attached").records.clone();
            let recv = rx.recv();
            let resync = self.system.sleep(FOLLOW_RESYNC);
            futures::pin_mut!(recv);
            match futures::future::select(recv, resync).await {
                // A live batch: reconcile by seq.
                futures::future::Either::Left((Ok(stream), _)) => {
                    if stream.from <= self.last {
                        let fresh: Vec<(Seq, Record)> = stream
                            .records
                            .into_iter()
                            .filter(|(seq, _)| *seq > self.last)
                            .collect();
                        if let Some((seq, _)) = fresh.last() {
                            self.last = *seq;
                            return Ok(fresh);
                        }
                    }
                    // A gap (`from > last`, a lag-drop) or all duplicates (a
                    // re-subscribe replay): fall through; the loop re-backfills.
                }
                // The stream closed (a clean step-down dropped the sink): re-attach.
                futures::future::Either::Left((Err(_), _)) => self.sub = None,
                // The timer won: a silent move/crash leaves the stream open but
                // dead. If the head advanced anyway, re-subscribe to the current
                // leader (the old sink is orphaned) and return the backfill; else
                // we are simply idle — keep the subscription, no churn.
                futures::future::Either::Right(_) => {
                    if let Some(batch) = self.backfill().await? {
                        self.sub = None;
                        return Ok(batch);
                    }
                }
            }
        }
    }

    /// One page of records after the cursor, read from the journal, advancing the
    /// cursor. `None` when already at the head.
    async fn backfill(&mut self) -> Result<Option<Vec<(Seq, Record)>>, GrainError> {
        let page = self
            .grain
            .ask(Tail {
                from: self.last,
                limit: FOLLOW_PAGE,
            })
            .await?;
        match page.last() {
            Some((seq, _)) => {
                self.last = *seq;
                Ok(Some(page))
            }
            None => Ok(None),
        }
    }
}

/// The ephemeral reply-to actor behind `prompt` and delegation (§7.4): it parks
/// on one [`RunCompleted`] notification, hands its outcome to a one-shot channel,
/// and stops. Its `ActorRef` is what rides in `Submit { reply_to }`; the run's
/// outcome is delivered to it whether the run is still live or already ended.
pub struct ReplyMailbox<S: HarnessSystem> {
    tx: Option<oneshot::Sender<RunOutcome>>,
    _marker: std::marker::PhantomData<S>,
}

impl<S: HarnessSystem> ReplyMailbox<S> {
    pub(crate) fn new(tx: oneshot::Sender<RunOutcome>) -> ReplyMailbox<S> {
        ReplyMailbox {
            tx: Some(tx),
            _marker: std::marker::PhantomData,
        }
    }
}

impl<S: HarnessSystem> Actor for ReplyMailbox<S> {
    type System = S;

    fn register(registry: &mut HandlerRegistry<Self>) {
        registry.accept::<RunCompleted>();
    }
}

impl<S: HarnessSystem> Handler<RunCompleted> for ReplyMailbox<S> {
    async fn handle(&mut self, msg: RunCompleted, ctx: &Ctx<Self>) {
        if let Some(tx) = self.tx.take() {
            let _ = tx.send(msg.outcome);
        }
        ctx.stop();
    }
}
