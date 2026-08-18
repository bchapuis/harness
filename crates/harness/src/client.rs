//! The client view (harness spec §7.4): [`Harness`], [`SessionRef`], and the
//! ephemeral reply-to actor behind the blocking `prompt` convenience.
//!
//! A session is a grain, so addressing is granary's: [`SessionRef`] wraps a
//! [`GrainRef<Agent>`](granary::GrainRef) and `ask`s it the §7.3 commands,
//! location-transparently (granary §4.3). [`Harness::cluster`] hosts one `Agent`
//! grain per kind via `granary_named` (each `KindId` is its own grain type,
//! §2.2), injecting the node's model and sandbox seams into every activation
//! through the factory.
//!
//! [`HarnessSystem`] is what the harness needs over [`GranarySystem`]: a way to
//! emit its §10.4 events onto the same observability stream the grain's events
//! ride.

use std::collections::BTreeMap;
use std::collections::BTreeSet;
use std::collections::HashMap;
use std::marker::PhantomData;
use std::sync::Arc;
use std::sync::LazyLock;
use std::sync::Mutex;
use std::sync::OnceLock;
use std::time::Duration;

use actor_cluster::ClusterSystem;
use actor_cluster::Transport;
use actor_core::Actor;
use actor_core::ActorRef;
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
use granary::GrainError;
use granary::GrainRef;
use granary::Granary;
use granary::GranaryExt;
use granary::GranaryNode;
use granary::GranarySystem;

use crate::agent::Agent;
use crate::agent::AttachOutcome;
use crate::agent::Cancel;
use crate::agent::RunCompleted;
use crate::agent::submit_and_attach;
use crate::kind::Kind;
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

/// Interned grain-type names, keyed by kind name (granary keys purely by string,
/// §5.1). A build re-tried in a discovery poll loop ([`Harness::client`]) reuses
/// the leaked name instead of leaking a fresh copy each attempt.
static GRAIN_TYPE_IDS: LazyLock<Mutex<HashMap<String, &'static str>>> =
    LazyLock::new(|| Mutex::new(HashMap::new()));

/// A grain type name must be `&'static` (the `GrainName` tag, §5.1); kinds are a
/// bounded deployment-time set, so interning one leaked `&'static str` per
/// distinct kind name is sound.
fn leak_grain_type(kind_id: &KindId) -> &'static str {
    let mut ids = GRAIN_TYPE_IDS
        .lock()
        .expect("grain-type name cache poisoned");
    if let Some(id) = ids.get(kind_id.as_str()) {
        return id;
    }
    let id: &'static str = Box::leak(kind_id.as_str().to_owned().into_boxed_str());
    ids.insert(kind_id.as_str().to_owned(), id);
    id
}

/// What the harness needs from the actor system beyond [`GranarySystem`]: a way
/// to emit its observability events (§10.4) onto the framework's stream — the
/// same stream the grain's events ride, so checkers see one ordered sequence.
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
    /// near-zero `max_tokens` call still pays its full input. Defaults to the
    /// default per-call `max_tokens` ([`ModelParams`](crate::ModelParams)), so a
    /// run stops rather than issue a final call that cannot fit a full-size
    /// response; set to 0 to disable the floor.
    pub budget_floor: u64,
    /// Floor of a delegating parent's total wait on a child run (§8.1): the
    /// wait is this floor plus [`child_wait_per_step`](Self::child_wait_per_step)
    /// × the child's carved `steps`, so even a zero-step carve gets a grace
    /// window for queueing and transport.
    pub child_wait_floor: Duration,
    /// Per-step allowance in the child wait (§8.1). The carve's `steps` bound
    /// the whole subtree's model calls (§9.1 item 3), so steps are the right
    /// scale for how long a child may legitimately take; the allowance covers
    /// one model call plus its tool round, so it SHOULD be at least the tool
    /// timeout.
    pub child_wait_per_step: Duration,
}

impl HarnessConfig {
    /// A delegating parent's total wait on a child run (§8.1), scaled to the
    /// child's carved budget: `child_wait_floor + child_wait_per_step × steps`.
    /// Past it the delegation resolves as a `ToolError` for the parent's model;
    /// the child run continues, its budget the backstop (§9.2 item 3).
    pub fn child_wait(&self, budget: &crate::Budget) -> Duration {
        self.child_wait_floor
            .saturating_add(self.child_wait_per_step.saturating_mul(budget.steps))
    }
}

impl Default for HarnessConfig {
    fn default() -> Self {
        HarnessConfig {
            submit_deadline: Duration::from_secs(30),
            tool_timeout: Duration::from_secs(300),
            budget_floor: crate::ModelParams::default().max_tokens,
            child_wait_floor: Duration::from_secs(600),
            child_wait_per_step: Duration::from_secs(300),
        }
    }
}

/// The node-global addressing context shared by a [`Harness`] and every `Agent`
/// activation it hosts (§7.4): the per-kind granary directory (for routing and
/// delegation), the cluster-wide kind registry, and the harness tuning. It holds
/// **no** activation seams — those live on the hosted kind's [`Seams`].
pub(crate) struct Shared<S: HarnessSystem> {
    pub(crate) kinds: Kinds,
    pub(crate) config: HarnessConfig,
    /// One `Granary` handle per kind, set once after every kind is built, so a
    /// grain can address children of any kind for delegation (§8.1) and cancel
    /// propagation (§9.2). Filled after construction — the factories that produce
    /// activations are captured before the handles exist.
    pub(crate) granaries: OnceLock<BTreeMap<KindId, Granary<Agent<S>>>>,
}

impl<S: HarnessSystem> Shared<S> {
    /// The per-kind `Granary` handles (set in [`HarnessBuilder::build`]).
    pub(crate) fn granaries(&self) -> &BTreeMap<KindId, Granary<Agent<S>>> {
        self.granaries
            .get()
            .expect("granaries set in HarnessBuilder::build")
    }
}

/// A hosted kind's activation seams (§4, §5.3): the node's model and sandbox,
/// injected into each activation of that kind. An `Agent` exists only for a
/// **hosted** kind, so the seams are always present — no `Option`. A
/// routing-only kind has no `Seams`: it never activates.
#[derive(Clone)]
pub(crate) struct Seams {
    pub(crate) model: Arc<dyn Model>,
    pub(crate) sandbox: Arc<dyn SandboxProvider>,
}

/// One node's view of the session-grain namespace (harness spec §7.4): it
/// addresses every kind's sessions, and for the kinds this node **hosts** it runs
/// their grain type with the node's model and sandbox seams (§4, §5.3). Built with
/// [`Harness::builder`]; cheap to clone, every clone shares the node's directory.
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
    /// Start building a harness on this node over the cluster-wide `kinds`
    /// registry (§7.1, identical on every node). Select which kinds this node
    /// hosts with [`host`](HarnessBuilder::host) / [`host_all`](HarnessBuilder::host_all)
    /// and which it only routes with [`route`](HarnessBuilder::route) /
    /// [`route_all`](HarnessBuilder::route_all), then `build`. The common full node
    /// hosts every kind:
    ///
    /// ```ignore
    /// let h = Harness::builder(system, &kinds).host_all(model, sandbox).build();
    /// ```
    ///
    /// A routing-only gateway tier (the Orleans cluster-client) routes every kind
    /// and polls until the hosts have gossiped in:
    ///
    /// ```ignore
    /// let h: Option<_> = Harness::builder(system, &kinds).route_all().build();
    /// ```
    pub fn builder(system: S, kinds: &Kinds) -> HarnessBuilder<S, NoRoutes> {
        HarnessBuilder {
            granary_node: system.granary_node(),
            system,
            kinds: kinds.clone(),
            config: HarnessConfig::default(),
            hosted: Vec::new(),
            routed: Vec::new(),
            _state: PhantomData,
        }
    }

    /// Build a harness that **hosts every** registered kind on this node — a full
    /// cluster member (the common case). Shorthand for
    /// `Harness::builder(system, kinds).host_all(model, sandbox).build()`; reach for
    /// the builder directly for a custom [`HarnessConfig`], or to host some kinds
    /// and route others.
    pub fn cluster(
        system: S,
        kinds: &Kinds,
        model: Arc<dyn Model>,
        sandbox: Arc<dyn SandboxProvider>,
    ) -> Harness<S> {
        Harness::builder(system, kinds)
            .host_all(model, sandbox)
            .build()
    }

    /// Build a routing-only **client** harness (the Orleans cluster-client): it
    /// hosts no grains, it only *addresses* sessions hosted elsewhere on the
    /// cluster. Shorthand for `Harness::builder(system, kinds).route_all().build()`.
    /// Returns `None` until every kind's host gateway has gossiped into this node's
    /// receptionist — the caller polls, as a node waits for its peers. The model
    /// params/tools in `kinds` are ignored (a client never activates); only the
    /// names and `GranaryConfig.shards` matter, and they MUST match the hosts'.
    pub fn client(system: S, kinds: &Kinds) -> Option<Harness<S>> {
        Harness::builder(system, kinds).route_all().build()
    }

    /// A client view of `session` under `kind` (harness spec §7.4). Pure:
    /// name→shard is a local hash, no I/O — the one failure is a directory
    /// miss, a kind this node neither hosts nor routes ([`UnknownKind`]).
    /// Kind names reach clients from outside (a URL path at the gateway), so
    /// the miss is an error value, not a panic. Creation is implicit in the
    /// first turn.
    pub fn session(&self, kind: &str, session: SessionId) -> Result<SessionRef<S>, UnknownKind> {
        let kind = KindId::new(kind);
        let Some(granary) = self.shared.granaries().get(&kind) else {
            return Err(UnknownKind { kind });
        };
        Ok(SessionRef {
            grain: granary.grain(session.as_str()),
            kind,
            session,
            system: self.system.clone(),
            config: self.shared.config.clone(),
        })
    }

    /// The actor system this harness runs on.
    pub fn system(&self) -> &S {
        &self.system
    }
}

/// The error of [`Harness::session`]: the named kind is in neither this node's
/// hosted nor its routed set, so there is no granary to address the session
/// through. Permanent for this node's directory — a retry cannot clear it; the
/// fix is configuration (host or route the kind) or the caller's spelling.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct UnknownKind {
    /// The kind the caller named.
    pub kind: KindId,
}

impl std::fmt::Display for UnknownKind {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "kind '{}' is neither hosted nor routed on this node",
            self.kind
        )
    }
}

impl std::error::Error for UnknownKind {}

/// [`HarnessBuilder`] type-state: no routed kinds yet, so `build` is infallible —
/// hosting starts no discovery.
pub struct NoRoutes;

/// [`HarnessBuilder`] type-state: at least one routed kind, so `build` returns
/// `Option` — `None` until every routed kind's host gateway has gossiped into this
/// node's receptionist.
pub struct HasRoutes;

/// Builds a [`Harness`], choosing **per kind** whether this node hosts the grain
/// type — starting its gateway, replica store, and shard-map group and injecting
/// the model/sandbox [`Seams`] into each activation — or only routes to a host of
/// it (the Orleans cluster-client path: no hosting, the gateway discovered through
/// the receptionist). See [`Harness::builder`].
pub struct HarnessBuilder<S: HarnessSystem, R = NoRoutes> {
    system: S,
    /// The node-scoped granary capabilities every hosted kind's grain type shares
    /// (granary §7.4, §13). One per node, not per kind — a node hosting five kinds
    /// wants one I/O pool and one metrics registry, not five.
    granary_node: GranaryNode<S>,
    kinds: Kinds,
    config: HarnessConfig,
    hosted: Vec<(KindId, Arc<Kind>, Seams)>,
    routed: Vec<(KindId, usize)>,
    _state: PhantomData<R>,
}

impl<S: HarnessSystem, R> HarnessBuilder<S, R> {
    /// The node-scoped granary capabilities every hosted kind shares (granary §7.4,
    /// §13): the blocking-I/O pool and the metrics registry. Defaults to the system's
    /// own — inline I/O and discarded metrics — which is what the simulation wants.
    ///
    /// A deployment on real storage sets it once here rather than per kind, since a
    /// pool exists to bound *the node's* concurrent device work.
    pub fn granary_node(mut self, node: GranaryNode<S>) -> Self {
        self.granary_node = node;
        self
    }

    /// Override the harness tuning (§7.2); defaults to [`HarnessConfig::default`].
    pub fn config(mut self, config: HarnessConfig) -> Self {
        self.config = config;
        self
    }

    /// Host the registered kind named `kind` on this node, injecting `model` and
    /// `sandbox` into each of its activations. Panics if `kind` is not in the
    /// registry this builder was seeded with (§7.1: a node hosts only kinds it
    /// knows).
    pub fn host(
        mut self,
        kind: &str,
        model: Arc<dyn Model>,
        sandbox: Arc<dyn SandboxProvider>,
    ) -> Self {
        let id = KindId::new(kind);
        let def = self
            .kinds
            .get(&id)
            .unwrap_or_else(|| panic!("kind '{kind}' is not registered"));
        self.hosted.push((id, def, Seams { model, sandbox }));
        self
    }

    /// Host every registered kind on this node with the same seams — the common
    /// case for a full node.
    pub fn host_all(mut self, model: Arc<dyn Model>, sandbox: Arc<dyn SandboxProvider>) -> Self {
        for (id, def) in self.kinds.iter() {
            self.hosted.push((
                id.clone(),
                Arc::clone(def),
                Seams {
                    model: Arc::clone(&model),
                    sandbox: Arc::clone(&sandbox),
                },
            ));
        }
        self
    }

    /// Route to a host of the registered kind named `kind` instead of hosting it
    /// (the Orleans cluster-client path). Shards come from the kind's
    /// `GranaryConfig` so a name hashes to the same shard as on the hosts. Panics
    /// if `kind` is not registered. Moves the builder to the `HasRoutes`
    /// type-state, so `build` then returns `Option`.
    pub fn route(self, kind: &str) -> HarnessBuilder<S, HasRoutes> {
        let id = KindId::new(kind);
        let shards = self
            .kinds
            .get(&id)
            .unwrap_or_else(|| panic!("kind '{kind}' is not registered"))
            .config
            .shards
            .max(1);
        let mut builder = self.into_has_routes();
        builder.routed.push((id, shards));
        builder
    }

    /// Route every registered kind — a routing-only tier (e.g. the gateway).
    pub fn route_all(self) -> HarnessBuilder<S, HasRoutes> {
        let routed: Vec<(KindId, usize)> = self
            .kinds
            .iter()
            .map(|(id, def)| (id.clone(), def.config.shards.max(1)))
            .collect();
        let mut builder = self.into_has_routes();
        builder.routed.extend(routed);
        builder
    }

    /// Retag to the `HasRoutes` type-state, carrying the accumulated builder.
    fn into_has_routes(self) -> HarnessBuilder<S, HasRoutes> {
        HarnessBuilder {
            system: self.system,
            granary_node: self.granary_node,
            kinds: self.kinds,
            config: self.config,
            hosted: self.hosted,
            routed: self.routed,
            _state: PhantomData,
        }
    }

    /// Resolve routed kinds, then host hosted kinds, then publish the directory.
    /// Resolving routed kinds first means a `None` return (one not yet discovered)
    /// starts no hosting, so a poll loop never double-hosts.
    ///
    /// Panics unless every hosted kind's delegation allowlist is contained in
    /// this node's directory (hosted ∪ routed, §7.1): the parent's node launches
    /// the child `Submit` (§8.1) and delivers owed cancels (§9.2) through its
    /// own granary for the child's kind, so an uncovered allowlist would fail
    /// every delegation per-call and leave a cancel owed until a
    /// better-configured node leads the shard. A deployment configuration
    /// error, surfaced as loudly as a duplicate tool name (§5.2).
    fn assemble(self) -> Option<Harness<S>> {
        let directory: BTreeSet<&KindId> = self
            .hosted
            .iter()
            .map(|(id, _, _)| id)
            .chain(self.routed.iter().map(|(id, _)| id))
            .collect();
        for (id, def, _) in &self.hosted {
            for child in &def.delegates {
                assert!(
                    directory.contains(child),
                    "kind '{id}' delegates to '{child}', which this node neither hosts nor routes"
                );
            }
        }
        let mut granaries: BTreeMap<KindId, Granary<Agent<S>>> = BTreeMap::new();
        for (id, shards) in &self.routed {
            let grain_type = leak_grain_type(id);
            let granary = self
                .system
                .granary_client::<Agent<S>>(grain_type, *shards)?;
            granaries.insert(id.clone(), granary);
        }
        let shared = Arc::new(Shared {
            kinds: self.kinds,
            config: self.config,
            granaries: OnceLock::new(),
        });
        // Host one grain type per hosted kind. The factory captures the node's
        // directory (via `shared`), this kind's definition, and its seams, and
        // builds a fresh activation per (re)activation — so the grain needs no
        // `Default` and no process-global; multi-node-in-one-process simulations
        // each get their own (§12.1).
        for (id, def, seams) in &self.hosted {
            let grain_type = leak_grain_type(id);
            let factory_shared = Arc::clone(&shared);
            let factory_kind = Arc::clone(def);
            let factory_seams = seams.clone();
            let granary = self.granary_node.granary_named::<Agent<S>>(
                grain_type,
                def.config.clone(),
                Arc::new(move || {
                    Agent::new(
                        Arc::clone(&factory_shared),
                        Arc::clone(&factory_kind),
                        factory_seams.clone(),
                    )
                }),
            );
            granaries.insert(id.clone(), granary);
        }
        shared
            .granaries
            .set(granaries)
            .unwrap_or_else(|_| panic!("granaries set once"));
        Some(Harness {
            shared,
            system: self.system,
        })
    }
}

impl<S: HarnessSystem> HarnessBuilder<S, NoRoutes> {
    /// Build the harness. Infallible: there is nothing to discover. Panics if a
    /// hosted kind's delegation allowlist names a kind this node neither hosts
    /// nor routes (§7.1) — a deployment configuration error.
    pub fn build(self) -> Harness<S> {
        self.assemble()
            .expect("a host-only build has no routed kinds to discover")
    }
}

impl<S: HarnessSystem> HarnessBuilder<S, HasRoutes> {
    /// Build the harness, or `None` until every routed kind's host gateway has
    /// gossiped into this node's receptionist (poll). Hosting side effects run
    /// only once all routed kinds resolve. Panics if a hosted kind's delegation
    /// allowlist names a kind this node neither hosts nor routes (§7.1) — a
    /// deployment configuration error.
    pub fn build(self) -> Option<Harness<S>> {
        self.assemble()
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
    /// §7.4): `Submit` (ack) plus awaiting the one `RunCompleted` notification.
    /// The deadline bounds this caller's **wait**, never the run: on a lapse it
    /// re-submits the same `TurnId`, which re-attaches or returns the recorded
    /// outcome (H7).
    pub async fn prompt(&self, turn: Turn) -> Result<RunOutcome, GrainError> {
        self.submit(turn, None, self.config.submit_deadline).await
    }

    /// [`prompt`](Self::prompt) with an explicit wait deadline.
    pub async fn prompt_within(
        &self,
        turn: Turn,
        within: Duration,
    ) -> Result<RunOutcome, GrainError> {
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

    /// Read committed records (harness spec §10.2): at most `limit` records
    /// after `from` (`limit` clamped to [`TAIL_PAGE`](crate::TAIL_PAGE)) — an
    /// idempotent, replication-free read of the leader's journal, riding
    /// granary's gateway-level event read (granary §7.5). It never
    /// get-or-activates the session, so **polling a hibernated session leaves
    /// it asleep** — observation costs a journal read, not a rehydration. The
    /// returned `Seq`s are the journal's real slots (facet records occupy
    /// interleaved ones); fewer than `limit` records come back only at the
    /// head, so page by re-asking from the last `Seq` returned. A leader-side
    /// read failure surfaces as the grain's `Unavailable`: transient, and — a
    /// read commits nothing — always safe to retry.
    pub async fn tail(&self, from: Seq, limit: u32) -> Result<Vec<(Seq, Record)>, GrainError> {
        self.grain.events(from, limit.min(TAIL_PAGE) as usize).await
    }

    /// Follow the session's records live from `from` (granary §7.9). See
    /// [`Follower`]; caller-driven, pull batches with [`Follower::next`].
    pub fn follow(&self, from: Seq) -> Follower<S> {
        Follower {
            grain: self.grain.clone(),
            system: self.system.clone(),
            sub: None,
            last: from,
        }
    }
}

/// One page bound for a caller's tail reads (§10.2): [`SessionRef::tail`]
/// clamps `limit` to it, so no single call asks the journal for an unbounded
/// reply. A caller wanting more pages by advancing `from` past the last `Seq`
/// returned. (The gateway additionally bounds each wire page server-side,
/// granary §7.5, so the clamp here is the API contract, not the safety.)
pub const TAIL_PAGE: u32 = 1024;

/// Journal page size for a follower's backfill reads.
const FOLLOW_PAGE: usize = 256;

/// How long a caught-up follower waits for a live record before re-checking the
/// journal. A silent leader move or crash leaves the old sink alive but idle —
/// the stream never closes and the new leader has no sink — so a periodic
/// backfill is the liveness net that detects the move and re-subscribes (grain §7.9).
const FOLLOW_RESYNC: Duration = Duration::from_secs(2);

/// A live follower over a session's journal (granary §7.9). It rides a grain
/// record subscription and reconciles by `Seq`: it backfills from the journal on
/// first attach, on any gap, and after the stream closes (a leader move, a
/// lag-drop, or hibernation), so [`next`](Self::next) yields the exact committed
/// sequence — in order, with no gap or duplicate (granary G16). Push is the fast
/// path; the journal is the authority.
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

    /// One page of records after the cursor, read from the journal (the
    /// non-activating gateway read, granary §7.5), advancing the cursor. `None`
    /// when already at the head.
    async fn backfill(&mut self) -> Result<Option<Vec<(Seq, Record)>>, GrainError> {
        let page = self.grain.events(self.last, FOLLOW_PAGE).await?;
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
/// Its lifetime is its caller's wait, not the delivery: an attempt that ends
/// without an outcome stops it through [`MailboxGuard`], so no path — a lapse,
/// a rejection, a transport failure, an abandoned caller — leaks the actor.
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
        // `RunCompleted` only: `Discard` stays local-only (§7.4) — the caller's
        // own guard stops the mailbox, never a peer.
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

/// Stop an ephemeral reply mailbox whose caller's wait has ended (§7.4).
/// Local-only: never in `register`'s allowlist, so no peer can stop another
/// caller's mailbox; only [`MailboxGuard`] sends it.
#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
pub(crate) struct Discard;

impl actor_core::Message for Discard {
    type Reply = ();
    const MANIFEST: actor_core::Manifest = actor_core::Manifest::new("harness.ReplyDiscard");
}

impl<S: HarnessSystem> Handler<Discard> for ReplyMailbox<S> {
    async fn handle(&mut self, _msg: Discard, ctx: &Ctx<Self>) {
        ctx.stop();
    }
}

/// Ties a [`ReplyMailbox`] to its caller's wait (§7.4): dropping the guard —
/// every exit from a submit-and-attach attempt, including the caller dropping
/// the future mid-await — launches a [`Discard`] at the mailbox. Disarmed on
/// the one path where the mailbox has already delivered and stopped itself; a
/// discard racing a late delivery is harmless either way (a `tell` to a
/// stopped actor fails quietly, and a lost notification is recovered by
/// re-contact, §7.3).
pub(crate) struct MailboxGuard<S: HarnessSystem> {
    system: S,
    mailbox: ActorRef<ReplyMailbox<S>>,
    armed: bool,
}

impl<S: HarnessSystem> MailboxGuard<S> {
    pub(crate) fn new(system: S, mailbox: ActorRef<ReplyMailbox<S>>) -> MailboxGuard<S> {
        MailboxGuard {
            system,
            mailbox,
            armed: true,
        }
    }

    /// The outcome was delivered: the mailbox stopped itself, nothing to stop.
    pub(crate) fn disarm(mut self) {
        self.armed = false;
    }
}

impl<S: HarnessSystem> Drop for MailboxGuard<S> {
    fn drop(&mut self) {
        if !self.armed {
            return;
        }
        let mailbox = self.mailbox.clone();
        self.system.launch(Box::pin(async move {
            let _ = mailbox.tell(Discard).await;
        }));
    }
}
