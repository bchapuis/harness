//! The client view (harness spec §7.4): [`Harness`] and [`SessionRef`].
//!
//! `SessionRef` is the deep module of the client side: one call hides owner
//! computation, receptionist lookup, host resolution, activation, and the
//! waiter hop the serial-executor model imposes (see `host.rs`). It never
//! transparently retries a failed `Submit` (core spec §1.2): a transport
//! failure surfaces as `CallError`, and the caller re-submits the same
//! `TurnId` at will — the explicit idempotency key core §7.2 prescribes
//! (invariant H7).
//!
//! [`HarnessSystem`] is the small adapter the harness needs over an actor
//! system: placement (util spec §2), task launching, the clock, and event
//! emission. Implemented for both `LocalSystem` (single node: it owns every
//! session — the shape the in-memory journal confines a deployment to anyway,
//! §6.1) and `ClusterSystem` (rendezvous placement over the serving set).

use std::sync::Arc;
use std::time::Duration;

use actor_cluster::ClusterSystem;
use actor_cluster::Transport;
use actor_core::ActorSystem;
use actor_core::BoxFuture;
use actor_core::CallError;
use actor_core::Clock;
use actor_core::Entropy;
use actor_core::Event;
use actor_core::LocalSystem;
use actor_core::NodeId;
use actor_core::Spawner;

use crate::host::Await;
use crate::host::Awaited;
use crate::host::Cancel;
use crate::host::Host;
use crate::host::HostReject;
use crate::host::Kinds;
use crate::host::Submit;
use crate::host::Tail;
use crate::host::TailFetch;
use crate::host::TailReader;
use crate::host::TurnWaiter;
use crate::host::host_key;
use crate::journal::Journal;
use crate::journal::SeqNo;
use crate::model::Model;
use crate::sandbox::SandboxProvider;
use crate::session::KindId;
use crate::session::Lineage;
use crate::session::Record;
use crate::session::RunOutcome;
use crate::session::SessionId;
use crate::session::Turn;
use crate::session::TurnId;

/// What the harness needs from the actor system beyond [`ActorSystem`]: the
/// runtime seam handles (core spec §4.6) and the placement function (util
/// spec §2). One trait, two implementations — the same move as the core's
/// own seams, so the harness runs unchanged on a single node, a cluster, or
/// the simulator.
pub trait HarnessSystem: ActorSystem {
    /// The system's clock type (core spec §4.6).
    type RuntimeClock: Clock;

    /// A handle to the system clock; the only time source the harness reads.
    fn runtime_clock(&self) -> Self::RuntimeClock;

    /// Launch a background task on the system's spawner — model calls, tool
    /// executions, journal appends (§3.2). Never an OS thread (§12.1).
    fn launch(&self, task: BoxFuture<'static, ()>);

    /// The owner of `key` per the local membership view (util spec §2): a
    /// pure function, computed without I/O. `None` when no member serves.
    /// Views may transiently disagree — placement is a routing function, not
    /// a lease (util spec §2.3); the journal fence carries exclusivity (§6.2).
    fn owner_of(&self, key: &[u8]) -> Option<NodeId>;

    /// Emit onto the system's observability stream (§10.4).
    fn emit_event(&self, event: Event);
}

impl<C: Clock, E: Entropy, S: Spawner> HarnessSystem for LocalSystem<C, E, S> {
    type RuntimeClock = C;

    fn runtime_clock(&self) -> C {
        self.clock().clone()
    }

    fn launch(&self, task: BoxFuture<'static, ()>) {
        self.spawner().launch(task);
    }

    fn owner_of(&self, _key: &[u8]) -> Option<NodeId> {
        // A single-node system owns every session.
        Some(self.node())
    }

    fn emit_event(&self, event: Event) {
        self.emit(event);
    }
}

impl<C: Clock, E: Entropy, S: Spawner, T: Transport> HarnessSystem for ClusterSystem<C, E, S, T> {
    type RuntimeClock = C;

    fn runtime_clock(&self) -> C {
        self.clock().clone()
    }

    fn launch(&self, task: BoxFuture<'static, ()>) {
        self.launch_task(task);
    }

    fn owner_of(&self, key: &[u8]) -> Option<NodeId> {
        self.place(key)
    }

    fn emit_event(&self, event: Event) {
        self.emit(event);
    }
}

/// Harness tuning (harness spec §7.2, §9.1): the few knobs the spec calls
/// configurable, with defaults sized for ordinary deployments. All timing is
/// in logical time (`Clock`).
#[derive(Clone, Debug)]
pub struct HarnessConfig {
    /// Idle stop (§7.2): a session with no live run for this long is
    /// deactivated, releasing the sandbox — the knob trading sandbox cost
    /// against workspace continuity.
    pub idle_timeout: Duration,
    /// Cadence of the host's deactivation sweep (§7.2).
    pub tick_interval: Duration,
    /// Default deadline bounding a caller's wait on `Submit` (§7.3) — never
    /// the run, which continues unaffected when the caller times out.
    pub submit_deadline: Duration,
    /// Default per-tool execution bound (§5.3 item 3), overridable per
    /// declaration.
    pub tool_timeout: Duration,
    /// Bounded retries absorbing a transient journal outage (§6.5): attempts
    /// per append/load, with exponential backoff from `append_backoff`.
    pub journal_attempts: u32,
    pub journal_backoff: Duration,
    /// The token floor below which no model call is issued (§9.1 item 2): a
    /// near-zero `max_tokens` call still pays its full input.
    pub budget_floor: u64,
}

impl Default for HarnessConfig {
    fn default() -> Self {
        HarnessConfig {
            idle_timeout: Duration::from_secs(60),
            tick_interval: Duration::from_secs(5),
            submit_deadline: Duration::from_secs(30),
            tool_timeout: Duration::from_secs(60),
            journal_attempts: 4,
            journal_backoff: Duration::from_millis(100),
            budget_floor: 0,
        }
    }
}

pub(crate) struct Shared<S: HarnessSystem> {
    pub system: S,
    pub kinds: Kinds,
    pub journal: Arc<dyn Journal>,
    pub model: Arc<dyn Model>,
    pub sandboxes: Arc<dyn SandboxProvider>,
    pub config: HarnessConfig,
}

/// One node's harness (harness spec §7.4): spawns the node's [`Host`] and
/// injects all three seams (§4, §6, §5.3) in one place. Cheap to clone; every
/// clone shares the node's host and seams.
pub struct Harness<S: HarnessSystem> {
    inner: Arc<Shared<S>>,
}

impl<S: HarnessSystem> Clone for Harness<S> {
    fn clone(&self) -> Self {
        Harness {
            inner: Arc::clone(&self.inner),
        }
    }
}

impl<S: HarnessSystem> Harness<S> {
    /// Stand up the harness on this node: spawn the host and register it
    /// under the cluster-wide hosts key (§7.2). Call once per node, with the
    /// same `kinds` and the same logical `journal` everywhere (§6.1, §7.1).
    pub fn new(
        system: S,
        kinds: Kinds,
        journal: Arc<dyn Journal>,
        model: Arc<dyn Model>,
        sandboxes: Arc<dyn SandboxProvider>,
    ) -> Harness<S> {
        Harness::with_config(
            system,
            kinds,
            journal,
            model,
            sandboxes,
            HarnessConfig::default(),
        )
    }

    /// [`Harness::new`] with explicit tuning.
    pub fn with_config(
        system: S,
        kinds: Kinds,
        journal: Arc<dyn Journal>,
        model: Arc<dyn Model>,
        sandboxes: Arc<dyn SandboxProvider>,
        config: HarnessConfig,
    ) -> Harness<S> {
        let harness = Harness {
            inner: Arc::new(Shared {
                system,
                kinds,
                journal,
                model,
                sandboxes,
                config,
            }),
        };
        let host = harness.system().spawn(Host::new(harness.clone()));
        harness.system().receptionist().register(host_key(), &host);
        harness
    }

    /// A client view of `session` under `kind` (harness spec §7.4). Pure:
    /// placement is a local function; no I/O, no failure case (cf. `resolve`,
    /// core spec §4.3). Creation is implicit in the session's first turn.
    pub fn session(&self, kind: &str, session: SessionId) -> SessionRef<S> {
        SessionRef {
            harness: self.clone(),
            kind: KindId::new(kind),
            session,
        }
    }

    pub(crate) fn system(&self) -> &S {
        &self.inner.system
    }

    pub(crate) fn kinds(&self) -> &Kinds {
        &self.inner.kinds
    }

    pub(crate) fn journal(&self) -> &Arc<dyn Journal> {
        &self.inner.journal
    }

    pub(crate) fn model(&self) -> &Arc<dyn Model> {
        &self.inner.model
    }

    pub(crate) fn sandboxes(&self) -> &Arc<dyn SandboxProvider> {
        &self.inner.sandboxes
    }

    pub(crate) fn config(&self) -> &HarnessConfig {
        &self.inner.config
    }

    pub(crate) fn clock(&self) -> S::RuntimeClock {
        self.inner.system.runtime_clock()
    }

    /// Resolve the host of `session`'s current owner (§7.2, §7.4): placement
    /// names the node, the receptionist listing names its host. Listing lag
    /// or no serving owner fails fast (§7.4); the `TurnId` makes the caller's
    /// retry safe once views converge.
    fn owner_host(&self, session: &SessionId) -> Result<actor_core::ActorRef<Host<S>>, CallError> {
        let owner = self
            .system()
            .owner_of(session.as_str().as_bytes())
            .ok_or(CallError::Unreachable)?;
        self.system()
            .receptionist()
            .lookup(host_key::<S>())
            .iter()
            .find(|host| host.id().node() == owner)
            .cloned()
            .ok_or(CallError::DeadLetter)
    }
}

/// A typed client handle to one session (harness spec §7.4).
pub struct SessionRef<S: HarnessSystem> {
    harness: Harness<S>,
    kind: KindId,
    session: SessionId,
}

impl<S: HarnessSystem> Clone for SessionRef<S> {
    fn clone(&self) -> Self {
        SessionRef {
            harness: self.harness.clone(),
            kind: self.kind.clone(),
            session: self.session.clone(),
        }
    }
}

impl<S: HarnessSystem> SessionRef<S> {
    /// The session's durable identity.
    pub fn id(&self) -> &SessionId {
        &self.session
    }

    /// Submit a turn and await its run's terminal outcome (harness spec
    /// §7.3, §7.4), bounded by the configured submit deadline. The deadline
    /// bounds this caller's **wait**, never the run: on `Timeout` or
    /// `Unreachable` the run continues, and re-calling with the same `TurnId`
    /// returns the recorded outcome or attaches to the live run (H7).
    pub async fn prompt(&self, turn: Turn) -> Result<RunOutcome, CallError> {
        let within = self.harness.config().submit_deadline;
        self.prompt_within(turn, within).await
    }

    /// [`prompt`](Self::prompt) with an explicit wait deadline.
    pub async fn prompt_within(
        &self,
        turn: Turn,
        within: Duration,
    ) -> Result<RunOutcome, CallError> {
        self.submit(turn, None, within).await
    }

    /// The submission protocol behind `prompt` and delegation (§8.1): route
    /// to the owner's host, attach to the per-turn waiter, await the outcome.
    /// Loops only on outcomes that are *not* failures — a waiter reporting
    /// the activation deactivated under it (`Lost`, e.g. a fence loss §6.2)
    /// re-submits the same `TurnId`, which dedups (H7). A reported transport
    /// failure surfaces to the caller; it is never transparently retried
    /// (core spec §1.2).
    pub(crate) async fn submit(
        &self,
        turn: Turn,
        parent: Option<Lineage>,
        within: Duration,
    ) -> Result<RunOutcome, CallError> {
        let clock = self.harness.clock();
        let started = clock.now();
        loop {
            // The deadline bounds the whole wait, including `Lost`
            // re-attachments: a permanently unservable session (say, a
            // journal that stays down) surfaces as `Timeout`, never a spin.
            let elapsed = clock.now().duration_since(started);
            let Some(remaining) = within.checked_sub(elapsed).filter(|d| !d.is_zero()) else {
                return Err(CallError::Timeout);
            };
            let host = self.harness.owner_host(&self.session)?;
            let ticket = match host
                .ask(Submit {
                    session: self.session.clone(),
                    kind: self.kind.clone(),
                    turn: turn.clone(),
                    parent: parent.clone(),
                })
                .await?
            {
                Ok(ticket) => ticket,
                // The session actor terminated while the op was being handed
                // over — an activation boundary. The host *replied*, so
                // nothing was submitted: retrying is not a transparent retry
                // of an unacknowledged send (core spec §1.2), just the next
                // attempt against the fresh activation.
                Err(HostReject::Busy) => {
                    clock.sleep(Duration::from_millis(50)).await;
                    continue;
                }
                Err(other) => return Err(reject_to_call_error(other)),
            };
            let waiter = self.harness.system().resolve::<TurnWaiter<S>>(ticket.actor);
            // The waiter enforces the deadline itself (it parks on the
            // outcome with `Clock::timeout`), so the ask is never dropped
            // mid-flight; the transport deadline only needs to outlast it.
            let asked = waiter
                .ask_timeout(
                    Await {
                        within_nanos: remaining.as_nanos() as u64,
                    },
                    remaining + Duration::from_secs(5),
                )
                .await?;
            match asked {
                Awaited::Outcome(outcome) => return Ok(outcome),
                Awaited::Rejected(reason) => return Err(CallError::System(reason)),
                Awaited::TimedOut => return Err(CallError::Timeout),
                // The activation went away under the waiter (deactivation,
                // fence loss, restart): nothing failed and nothing was
                // reported to the caller yet — re-submit the same TurnId and
                // attach to wherever the session lives now.
                Awaited::Lost => continue,
            }
        }
    }

    /// Cancel the run `turn` names (harness spec §7.3, §9.2): idempotent — a
    /// cancel naming an ended or unknown run is a no-op, so one delayed in
    /// flight never kills the named run's successor.
    pub async fn cancel(&self, turn: &TurnId) -> Result<(), CallError> {
        let host = self.harness.owner_host(&self.session)?;
        host.ask(Cancel {
            session: self.session.clone(),
            turn: turn.clone(),
        })
        .await?
        .map_err(reject_to_call_error)
    }

    /// Read committed records (harness spec §10.2): at most `limit` records
    /// after `from`. An idempotent, fence-free journal read routed through
    /// the owner — one routing path — that never activates the session.
    pub async fn tail(&self, from: SeqNo, limit: u32) -> Result<Vec<(SeqNo, Record)>, CallError> {
        let host = self.harness.owner_host(&self.session)?;
        let ticket = host
            .ask(Tail {
                session: self.session.clone(),
                from,
                limit,
            })
            .await?
            .map_err(reject_to_call_error)?;
        let reader = self.harness.system().resolve::<TailReader<S>>(ticket.actor);
        reader.ask(TailFetch {}).await?.map_err(CallError::System)
    }
}

/// Map a host's synchronous routing rejection onto the transport error the
/// spec names for it (§7.2, §7.4).
fn reject_to_call_error(reject: HostReject) -> CallError {
    match reject {
        // The sender computed placement on a divergent view (§7.2): fail
        // fast; the TurnId makes the retry safe once views converge.
        HostReject::NotOwner => CallError::DeadLetter,
        HostReject::UnknownKind(kind) => {
            CallError::System(format!("kind not registered on this node: {kind}"))
        }
        // Reached only from one-shot operations (cancel, tail): the
        // submission path retries a Busy internally.
        HostReject::Busy => CallError::DeadLetter,
    }
}
