//! The client view (agent spec §2.5): [`Harness`], [`SessionRef`], and the
//! ephemeral reply-to actor behind the blocking `prompt` convenience.
//!
//! A session is an entity, so addressing is entity's: [`SessionRef`] wraps a
//! [`EntityRef<Agent>`](entity::EntityRef) and `ask`s it the §2.6 commands,
//! location-transparently (entity §2.2). [`Harness::cluster`] hosts one `Agent`
//! entity per kind via `entities_named` (each `KindId` is its own entity type,
//! §3.1), injecting the node's model and sandbox seams into every activation
//! through the factory.

use std::collections::BTreeMap;
use std::collections::BTreeSet;
use std::collections::HashMap;
use std::marker::PhantomData;
use std::sync::Arc;
use std::sync::LazyLock;
use std::sync::Mutex;
use std::sync::OnceLock;
use std::time::Duration;

use actor_core::Actor;
use actor_core::ActorRef;
use actor_core::Backoff;
use actor_core::CallError;
use actor_core::Ctx;
use actor_core::Handler;
use actor_core::HandlerRegistry;
use entity::Entities;
use entity::EntityError;
use entity::EntityExt;
use entity::EntityRef;
use entity::EntitySystem;
use entity::EventHistory;
use entity::Seq;
use entity::cluster::EntityNode;
use futures::channel::oneshot;
use futures::future::Either;
use futures::future::select;
use serde_json::Value;

use crate::agent::Agent;
use crate::agent::Cancel;
use crate::agent::RunCompleted;
use crate::agent::Submit;
use crate::agent::SubmitReject;
use crate::agent::TurnOutcome;
use crate::config::HarnessConfig;
use crate::kind::Kind;
use crate::kind::Kinds;
use crate::model::Model;
use crate::sandbox::SandboxProvider;
use crate::session::ChildRef;
use crate::session::KindId;
use crate::session::Lineage;
use crate::session::Record;
use crate::session::RunOutcome;
use crate::session::SessionId;
use crate::session::Turn;
use crate::session::TurnId;
use crate::tool::ToolError;

/// Interned entity-type names, keyed by kind name (entity keys purely by string,
/// §4.1). A build re-tried in a discovery poll loop ([`Harness::client`]) reuses
/// the leaked name instead of leaking a fresh copy each attempt.
static ENTITY_TYPE_IDS: LazyLock<Mutex<HashMap<String, &'static str>>> =
    LazyLock::new(|| Mutex::new(HashMap::new()));

/// An entity type name must be `&'static` (the `EntityName` tag, §4.1); kinds are a
/// bounded deployment-time set, so interning one leaked `&'static str` per
/// distinct kind name is sound.
fn leak_entity_type(kind_id: &KindId) -> &'static str {
    let mut ids = ENTITY_TYPE_IDS
        .lock()
        .expect("entity-type name cache poisoned");
    if let Some(id) = ids.get(kind_id.as_str()) {
        return id;
    }
    let id: &'static str = Box::leak(kind_id.as_str().to_owned().into_boxed_str());
    ids.insert(kind_id.as_str().to_owned(), id);
    id
}

/// The system a harness runs on: exactly an [`EntitySystem`] (the agent is an
/// entity, §3.1; its events ride [`ActorSystem::emit`]). The name is the
/// harness's own vocabulary for the requirement, so a consumer writes
/// `S: HarnessSystem` without naming the storage layer; the blanket impl makes
/// every entity-hosting system one.
///
/// [`ActorSystem::emit`]: actor_core::ActorSystem::emit
pub trait HarnessSystem: EntitySystem {}

impl<S: EntitySystem> HarnessSystem for S {}

/// The node-global addressing context shared by a [`Harness`] and every `Agent`
/// activation it hosts (§5.1): the per-kind entity directory (for routing and
/// delegation), the cluster-wide kind registry, and the agent runtime tuning. It holds
/// **no** activation seams: those live on the hosted kind's [`Seams`].
pub(crate) struct Shared<S: HarnessSystem> {
    pub(crate) kinds: Kinds,
    pub(crate) config: HarnessConfig,
    /// One `Entities` handle per kind, set once after every kind is built, so an
    /// entity can address children of any kind for delegation (§6.1) and cancel
    /// propagation (§7.2). Filled after construction: the factories that produce
    /// activations are captured before the handles exist.
    pub(crate) hosts: OnceLock<BTreeMap<KindId, Entities<Agent<S>>>>,
}

impl<S: HarnessSystem> Shared<S> {
    /// The per-kind `Entities` handles (set in [`HarnessBuilder::build`]).
    pub(crate) fn hosts(&self) -> &BTreeMap<KindId, Entities<Agent<S>>> {
        self.hosts
            .get()
            .expect("hosts set in HarnessBuilder::build")
    }
}

/// A hosted kind's activation seams (§2.1, §2.3): the node's model and sandbox,
/// injected into each activation of that kind. An `Agent` exists only for a
/// **hosted** kind, so the seams are always present, no `Option`. A
/// routing-only kind has no `Seams`: it never activates.
#[derive(Clone)]
pub(crate) struct Seams {
    pub(crate) model: Arc<dyn Model>,
    pub(crate) sandbox: Arc<dyn SandboxProvider>,
}

/// One node's view of the session-entity namespace (agent spec §2.5): it
/// addresses every kind's sessions, and for the kinds this node **hosts** it runs
/// their entity type with the node's model and sandbox seams (§2.1, §2.3). Built with
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
    /// registry (§5.1, identical on every node). Select which kinds this node
    /// hosts with [`host`](HarnessBuilder::host) / [`host_all`](HarnessBuilder::host_all)
    /// and which it only routes with
    /// [`route_kind`](HarnessBuilder::route_kind), then `build`. The common
    /// full node hosts every kind:
    ///
    /// ```ignore
    /// let h = Harness::builder(system, &kinds).host_all(model, sandbox).build();
    /// ```
    ///
    /// A routing-only gateway tier (the Orleans cluster-client) names the kinds
    /// it routes instead; see [`Harness::client`].
    pub fn builder(system: S, kinds: &Kinds) -> HarnessBuilder<S, NoRoutes> {
        HarnessBuilder {
            entity_node: system.entity_node(),
            system,
            kinds: kinds.clone(),
            config: HarnessConfig::default(),
            hosted: Vec::new(),
            routed: Vec::new(),
            _state: PhantomData,
        }
    }

    /// Build a harness that **hosts every** registered kind on this node: a full
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
    /// hosts no entities, it only *addresses* sessions hosted elsewhere on the
    /// cluster. `kinds` names the kinds it routes to, the one fact a router needs;
    /// prompts, tools, model params, and shard counts stay the hosts' concern.
    /// Returns `None` until every kind's host gateway has gossiped into this
    /// node's receptionist: the caller polls, as a node waits for its peers.
    pub fn client(system: S, kinds: &[String]) -> Option<Harness<S>> {
        let mut builder = Harness::builder(system, &Kinds::new()).into_has_routes();
        for kind in kinds {
            builder = builder.route_kind(kind);
        }
        builder.build()
    }

    /// A client view of `session` under `kind` (agent spec §2.5). Pure:
    /// name→shard is a local hash, no I/O; the one failure is a directory
    /// miss, a kind this node neither hosts nor routes ([`UnknownKind`]).
    /// Kind names reach clients from outside (a URL path at the gateway), so
    /// the miss is an error value, not a panic. Creation is implicit in the
    /// first turn.
    pub fn session(&self, kind: &str, session: SessionId) -> Result<SessionRef<S>, UnknownKind> {
        let kind = KindId::new(kind);
        let Some(entities) = self.shared.hosts().get(&kind) else {
            return Err(UnknownKind { kind });
        };
        Ok(SessionRef {
            entity: entities.entity(session.as_str()),
            kind,
            system: self.system.clone(),
            config: self.shared.config.clone(),
        })
    }
}

/// The error of [`Harness::session`]: the named kind is in neither this node's
/// hosted nor its routed set, so there is no entity to address the session
/// through. Permanent for this node's directory: a retry cannot clear it; the
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

/// The error of [`SessionRef::prompt`]: the submit was refused, or it could not
/// be delivered or committed. The two arms want opposite handling, so they stay
/// apart: a retry can clear an `Entity` failure, never a `Rejected` one.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PromptError {
    /// The session refused the turn (§5.3): a reused `TurnId` with different
    /// content, or a kind that is not the entity's type. A caller bug, and
    /// permanent: the same submit always rejects the same way.
    Rejected(SubmitReject),
    /// Reaching the session or committing the turn failed, or the wait lapsed
    /// (`Call(Timeout)`). Ambiguous: re-submitting the same `TurnId` is safe
    /// and re-attaches or returns the recorded outcome (A6).
    Entity(EntityError),
}

impl From<EntityError> for PromptError {
    fn from(e: EntityError) -> Self {
        PromptError::Entity(e)
    }
}

impl std::fmt::Display for PromptError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            PromptError::Rejected(reject) => write!(f, "turn rejected: {reject}"),
            PromptError::Entity(e) => write!(f, "{e}"),
        }
    }
}

impl std::error::Error for PromptError {}

/// [`HarnessBuilder`] type-state: no routed kinds yet, so `build` is infallible,
/// hosting starts no discovery.
pub struct NoRoutes;

/// [`HarnessBuilder`] type-state: at least one routed kind, so `build` returns
/// `Option`; `None` until every routed kind's host gateway has gossiped into this
/// node's receptionist.
pub struct HasRoutes;

/// Builds a [`Harness`], choosing **per kind** whether this node hosts the entity
/// type (starting its gateway, replica store, and shard-map group and injecting
/// the model/sandbox [`Seams`] into each activation), or only routes to a host of
/// it (the Orleans cluster-client path: no hosting, the gateway discovered through
/// the receptionist). See [`Harness::builder`].
pub struct HarnessBuilder<S: HarnessSystem, R = NoRoutes> {
    system: S,
    /// The node-scoped entity capabilities every hosted kind's entity type
    /// shares (storage §2.2, entity §10): one per node, not per kind.
    entity_node: EntityNode<S>,
    kinds: Kinds,
    config: HarnessConfig,
    hosted: Vec<(KindId, Arc<Kind>, Seams)>,
    routed: Vec<KindId>,
    _state: PhantomData<R>,
}

impl<S: HarnessSystem, R> HarnessBuilder<S, R> {
    /// The node-scoped entity capabilities every hosted kind shares (storage §2.2,
    /// entity §10): the blocking-I/O pool and the metrics registry. Defaults to the system's
    /// own (inline I/O and discarded metrics) which is what the simulation wants.
    ///
    /// A deployment on real storage sets it once here rather than per kind, since a
    /// pool exists to bound *the node's* concurrent device work. The node also
    /// records each kind hosted through it, so a deployment that stops gracefully
    /// passes the node it will call [`EntityNode::hand_off_leadership`] on.
    pub fn entity_node(mut self, node: EntityNode<S>) -> Self {
        self.entity_node = node;
        self
    }

    /// Override the agent runtime tuning (§5.2); defaults to [`HarnessConfig::default`].
    pub fn config(mut self, config: HarnessConfig) -> Self {
        self.config = config;
        self
    }

    /// Host the registered kind named `kind` on this node, injecting `model` and
    /// `sandbox` into each of its activations. Panics if `kind` is not in the
    /// registry this builder was seeded with (§5.1: a node hosts only kinds it
    /// knows). The panic is raised here rather than at `build`, so it points
    /// at the line that names the kind.
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

    /// Host every registered kind on this node with the same seams: the common
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

    /// Route to a host of the kind named `kind` instead of hosting it (the
    /// Orleans cluster-client path). Moves the builder to the `HasRoutes`
    /// type-state, so `build` then returns `Option`.
    pub fn route_kind(self, kind: &str) -> HarnessBuilder<S, HasRoutes> {
        let mut builder = self.into_has_routes();
        builder.routed.push(KindId::new(kind));
        builder
    }

    /// Retag to the `HasRoutes` type-state, carrying the accumulated builder.
    fn into_has_routes(self) -> HarnessBuilder<S, HasRoutes> {
        HarnessBuilder {
            system: self.system,
            entity_node: self.entity_node,
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
    /// this node's directory (hosted ∪ routed, §5.1): the parent's node launches
    /// the child `Submit` (§6.1) and delivers owed cancels (§7.2) through its
    /// own entity for the child's kind, so an uncovered allowlist would fail
    /// every delegation per-call and leave a cancel owed until a
    /// better-configured node leads the shard. A deployment configuration
    /// error, surfaced as loudly as a duplicate tool name (§2.4).
    fn assemble(self) -> Option<Harness<S>> {
        let directory: BTreeSet<&KindId> = self
            .hosted
            .iter()
            .map(|(id, _, _)| id)
            .chain(&self.routed)
            .collect();
        for (id, def, _) in &self.hosted {
            for child in &def.delegates {
                assert!(
                    directory.contains(child),
                    "kind '{id}' delegates to '{child}', which this node neither hosts nor routes"
                );
            }
        }
        let mut hosts: BTreeMap<KindId, Entities<Agent<S>>> = BTreeMap::new();
        for id in &self.routed {
            let entity_type = leak_entity_type(id);
            let entities = self.system.entities_client::<Agent<S>>(entity_type)?;
            hosts.insert(id.clone(), entities);
        }
        let shared = Arc::new(Shared {
            kinds: self.kinds,
            config: self.config,
            hosts: OnceLock::new(),
        });
        // Host one entity type per hosted kind. The factory captures the node's
        // directory (via `shared`), this kind's definition, and its seams, and
        // builds a fresh activation per (re)activation, so the entity needs no
        // `Default` and no process-global; multi-node-in-one-process simulations
        // each get their own (§2.2).
        for (id, def, seams) in &self.hosted {
            let entity_type = leak_entity_type(id);
            let factory_shared = Arc::clone(&shared);
            let factory_kind = Arc::clone(def);
            let factory_seams = seams.clone();
            let entities = self.entity_node.entities_named::<Agent<S>>(
                entity_type,
                def.entity.clone(),
                Arc::new(move || {
                    Agent::new(
                        Arc::clone(&factory_shared),
                        Arc::clone(&factory_kind),
                        factory_seams.clone(),
                    )
                }),
            );
            hosts.insert(id.clone(), entities);
        }
        shared
            .hosts
            .set(hosts)
            .unwrap_or_else(|_| panic!("hosts set once"));
        Some(Harness {
            shared,
            system: self.system,
        })
    }
}

impl<S: HarnessSystem> HarnessBuilder<S, NoRoutes> {
    /// Build the agent runtime. Infallible: there is nothing to discover. Panics if a
    /// hosted kind's delegation allowlist names a kind this node neither hosts
    /// nor routes (§5.1): a deployment configuration error.
    pub fn build(self) -> Harness<S> {
        self.assemble()
            .expect("a host-only build has no routed kinds to discover")
    }
}

impl<S: HarnessSystem> HarnessBuilder<S, HasRoutes> {
    /// Build the agent runtime, or `None` until every routed kind's host gateway has
    /// gossiped into this node's receptionist (poll). Hosting side effects run
    /// only once all routed kinds resolve. Panics if a hosted kind's delegation
    /// allowlist names a kind this node neither hosts nor routes (§5.1): a
    /// deployment configuration error.
    pub fn build(self) -> Option<Harness<S>> {
        self.assemble()
    }
}

/// A typed client handle to one session (agent spec §2.5): a thin agent-facing
/// surface over [`EntityRef<Agent>`](entity::EntityRef).
pub struct SessionRef<S: HarnessSystem> {
    entity: EntityRef<Agent<S>>,
    kind: KindId,
    system: S,
    config: HarnessConfig,
}

impl<S: HarnessSystem> Clone for SessionRef<S> {
    fn clone(&self) -> Self {
        SessionRef {
            entity: self.entity.clone(),
            kind: self.kind.clone(),
            system: self.system.clone(),
            config: self.config.clone(),
        }
    }
}

impl<S: HarnessSystem> SessionRef<S> {
    /// Submit a turn and await its run's terminal outcome (agent spec §2.6,
    /// §5.3): `Submit` (ack) plus awaiting the one `RunCompleted` notification,
    /// within the runtime's `submit_deadline`. The deadline bounds this
    /// caller's **wait**, never the run: a lapse returns
    /// `PromptError::Entity(EntityError::Call(CallError::Timeout))` and the run
    /// continues; re-prompting the same `TurnId` re-attaches or returns the
    /// recorded outcome (A6).
    pub async fn prompt(&self, turn: Turn) -> Result<RunOutcome, PromptError> {
        self.prompt_within(turn, self.config.submit_deadline).await
    }

    /// [`prompt`](Self::prompt) with an explicit wait deadline: `Submit` the
    /// turn carrying an ephemeral reply-to mailbox and await the one
    /// `RunCompleted`, the whole wait one attempt bounded by `within`. When
    /// the deadline lapses the call returns
    /// `PromptError::Entity(EntityError::Call(CallError::Timeout))`; only a
    /// mailbox that drops without an outcome before the deadline re-submits
    /// the same `TurnId`, re-registering on whatever activation now hosts the
    /// session. A failed `Submit` is **not** transparently retried (entity
    /// §1.1): an ambiguous transport failure surfaces as
    /// [`PromptError::Entity`], a permanent rejection as
    /// [`PromptError::Rejected`]. (Delegation submits through
    /// [`submit_and_attach`] directly, carrying its lineage.)
    pub async fn prompt_within(
        &self,
        turn: Turn,
        within: Duration,
    ) -> Result<RunOutcome, PromptError> {
        let started = self.system.now();
        loop {
            let elapsed = self.system.now().duration_since(started);
            let Some(remaining) = within.checked_sub(elapsed).filter(|d| !d.is_zero()) else {
                return Err(EntityError::Call(CallError::Timeout).into());
            };
            match submit_and_attach(&self.system, &self.entity, &self.kind, &turn, None, remaining)
                .await
            {
                AttachOutcome::Completed(outcome) => return Ok(outcome),
                // Mailbox dropped or wait lapsed: re-submit the same TurnId.
                AttachOutcome::Lapsed => continue,
                AttachOutcome::Rejected(reject) => return Err(PromptError::Rejected(reject)),
                AttachOutcome::Unreachable(e) => return Err(e.into()),
            }
        }
    }

    /// Cancel the run `turn` names (agent spec §2.6, §7.2): idempotent.
    pub async fn cancel(&self, turn: &TurnId) -> Result<(), EntityError> {
        self.entity.ask(Cancel { turn: turn.clone() }).await
    }

    /// The recorded outcome of the turn `turn` names (agent spec §2.5): `None`
    /// while it is queued or running, or when no such turn was accepted.
    /// Activates the session, unlike [`tail`](Self::tail).
    pub async fn outcome(&self, turn: &TurnId) -> Result<Option<RunOutcome>, EntityError> {
        self.entity.ask(TurnOutcome { turn: turn.clone() }).await
    }

    /// Read committed records (agent spec §8.2): at most `limit` records
    /// after `from` (`limit` clamped to [`TAIL_PAGE`]); an
    /// idempotent, replication-free read of the leader's journal, riding
    /// entity's gateway-level event read (entity §6). It never
    /// get-or-activates the session, so **polling a hibernated session leaves
    /// it asleep**: observation costs a journal read, not a rehydration. The
    /// returned `Seq`s are the journal's real slots (facet records occupy
    /// interleaved ones); fewer than `limit` records come back only at the
    /// head, so page by re-asking from the last `Seq` returned. A leader-side
    /// read failure surfaces as the entity's `Unavailable`: transient, and
    /// always safe to retry, since a read commits nothing.
    ///
    /// The returned [`EventHistory`]'s `base` reports how far snapshot
    /// compaction has truncated the journal (storage §8): a `from` below it
    /// asked for records the session's snapshot has subsumed, and the records
    /// in `(from, base]` are not (and can no longer be) served. `base <=
    /// from` means the page is complete history from `from`; without the base,
    /// truncation would be indistinguishable from the facet slots the
    /// projection legally skips.
    pub async fn tail(&self, from: Seq, limit: u32) -> Result<EventHistory<Record>, EntityError> {
        self.entity
            .events(from, limit.min(TAIL_PAGE) as usize)
            .await
    }

    /// Follow the session's records after `from` live (§8.2): the entity's
    /// follower over the session (entity §6.3). Caller-driven: pull batches
    /// with its `next`.
    pub fn follow(&self, from: Seq) -> Follower<S> {
        self.entity.follow(from)
    }
}

/// One page bound for a caller's tail reads (§8.2): [`SessionRef::tail`]
/// clamps `limit` to it, so no single call asks the journal for an unbounded
/// reply. A caller wanting more pages advances `from` past the last `Seq`
/// returned. (The gateway also bounds each wire page server-side, entity §6,
/// so the clamp here is the API contract, not the safety.)
pub const TAIL_PAGE: u32 = 1024;

/// A live follower over a session's records (§8.2): entity's **E16**
/// reconciler, yielding the exact committed record sequence, or
/// `Followed::Truncated` where compaction outran the cursor.
pub type Follower<S> = entity::Follower<Agent<S>>;

/// What one `next` on a [`Follower`] yields: the next records, or the
/// compaction base the cursor jumped to.
pub type Followed = entity::Followed<Record>;

/// The ephemeral reply-to actor behind `prompt` and delegation (§5.3): it parks
/// on one [`RunCompleted`] notification, hands its outcome to a one-shot channel,
/// and stops. Its `ActorRef` is what rides in `Submit { reply_to }`; the run's
/// outcome is delivered to it whether the run is still live or already ended.
/// Its lifetime is its caller's wait, not the delivery: an attempt that ends
/// without an outcome stops it through [`MailboxGuard`], so no path (a lapse,
/// a rejection, a transport failure, an abandoned caller) leaks the actor.
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
        // `RunCompleted` only: `Discard` stays local-only (§5.3); the caller's
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

/// Stop an ephemeral reply mailbox whose caller's wait has ended (§5.3).
/// Local-only: never in `register`'s allowlist, so no peer can stop another
/// caller's mailbox; only [`MailboxGuard`] sends it.
#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
pub(crate) struct Discard;

impl actor_core::Message for Discard {
    type Reply = ();
    const MANIFEST: actor_core::Manifest = actor_core::Manifest::new("agent.ReplyDiscard");
}

impl<S: HarnessSystem> Handler<Discard> for ReplyMailbox<S> {
    async fn handle(&mut self, _msg: Discard, ctx: &Ctx<Self>) {
        ctx.stop();
    }
}

/// Ties a [`ReplyMailbox`] to its caller's wait (§5.3): dropping the guard
/// (every exit from a submit-and-attach attempt, including the caller dropping
/// the future mid-await) launches a [`Discard`] at the mailbox. Disarmed on
/// the one path where the mailbox has already delivered and stopped itself; a
/// discard racing a late delivery is harmless either way (a `tell` to a
/// stopped actor fails quietly, and a lost notification is recovered by
/// re-contact, §2.6).
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

// ===========================================================================
// The attach protocol (§5.3, §6.1): one submit-and-await attempt, and the two
// loops over it.
// ===========================================================================

/// How many *consecutive* times an unreachable child is retried before a
/// delegation gives up (§6.1): past it the child surfaces as a tool failure
/// (§4.2), with the child's budget as the backstop (§7.2 item 3). A transport
/// bound, backed off between tries; any successful contact resets it. Distinct
/// from the wait on a reachable-but-slow child, which is budget-scaled config
/// ([`HarnessConfig::child_wait`], §6.1).
const TRANSPORT_RETRIES: u32 = 32;

/// The outcome of one [`submit_and_attach`] attempt. The *policy* (when to give
/// up, when to retry) is the caller's loop: the two callers bound it differently
/// (§5.3, §6.1).
pub(crate) enum AttachOutcome {
    /// The run finished and delivered its outcome.
    Completed(RunOutcome),
    /// The submit was accepted but the wait lapsed (or the mailbox dropped):
    /// re-submitting the same `TurnId` re-attaches (A6).
    Lapsed,
    /// The submit was permanently rejected (§5.3): never worth retrying.
    Rejected(SubmitReject),
    /// The entity was unreachable: a transport failure a retry might clear.
    Unreachable(EntityError),
}

/// One submit-and-await-outcome attempt against an entity (§5.3): spawn an
/// ephemeral reply mailbox, `Submit` the turn carrying it, and await the run's
/// `RunCompleted` bounded by `within`. Wrapping it in a retry loop is safe
/// because re-submitting the same `TurnId` is idempotent (A6). The mailbox
/// lives exactly as long as the attempt: a [`MailboxGuard`] stops it on every
/// path that ends without an outcome (including this future being dropped),
/// so a re-attach replaces the mailbox rather than accumulating one per
/// attempt (§5.3).
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

/// Submit to a child and await its outcome (§6.1 step 2), with the derived
/// `TurnId` keeping every re-attempt safe (A6). Re-attaches on the `cadence`
/// (each lapse re-registers on whatever activation now hosts the child, §5.4)
/// until the budget-scaled `wait` lapses; retries a consecutively unreachable
/// child up to [`TRANSPORT_RETRIES`] with backoff. Maps the child's
/// `RunOutcome` onto this delegation's tool outcome (§4.2). A give-up resolves
/// the call only for the parent: the child run continues, its budget the
/// backstop (§7.2 item 3).
pub(crate) async fn run_child<S: HarnessSystem>(
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
                system.sleep(child_backoff(unreachable)).await;
            }
        }
    }
}

/// Exponential backoff between transport retries of a child submit, capped
/// (core §2.4).
fn child_backoff(attempt: u32) -> Duration {
    Backoff::Exponential {
        base: Duration::from_millis(200),
        max: Duration::from_secs(2),
    }
    .delay(attempt)
}
