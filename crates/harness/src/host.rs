//! The host: kinds, activation, and the wire contract (harness spec §7).
//!
//! Every node runs one [`Host`] actor under the receptionist key
//! `harness.hosts` (§7.2). A session's owner is a pure function of the local
//! membership view; the host activates lazily on the first message for a
//! session it owns, deactivates on ownership moves and idleness, and rejects
//! fast when its view does not name it owner (§7.2) — routing discipline,
//! not a safety requirement: the journal fence protects the transcript
//! regardless of who appends (§6.2).
//!
//! **Why tickets.** A handler's reply is its return value on a serial
//! executor (core spec §6): an actor that awaited a run's completion inside a
//! handler would stall every other message it serves. A run outlives any
//! polite deadline, so the host parks nobody: `Submit` returns a [`Ticket`]
//! naming a per-call [`TurnWaiter`] whose *only* job is to park on that one
//! outcome, and `Tail` a [`TailReader`] that performs the journal read on its
//! own executor. [`SessionRef`](crate::client::SessionRef) hides the second
//! hop, so the caller sees exactly the §7.3 contract: ask `Submit`, receive
//! `Result<Completion, RunError>`, deadline bounding the wait and never the
//! run.

use std::collections::BTreeMap;
use std::collections::BTreeSet;
use std::sync::Arc;
use std::time::Duration;

use actor_core::Actor;
use actor_core::ActorId;
use actor_core::ActorRef;
use actor_core::Clock;
use actor_core::Ctx;
use actor_core::Handler;
use actor_core::HandlerRegistry;
use actor_core::Key;
use actor_core::Manifest;
use actor_core::Message;
use actor_core::Terminated;
use futures::channel::oneshot;
use serde::Deserialize;
use serde::Serialize;
use serde_json::Value;

use crate::agent::AgentActor;
use crate::agent::SubmitOp;
use crate::budget::Budget;
use crate::client::Harness;
use crate::client::HarnessSystem;
use crate::journal::SeqNo;
use crate::model::ModelParams;
use crate::sandbox::SandboxProfile;
use crate::sandbox::Tier;
use crate::session::KindId;
use crate::session::Lineage;
use crate::session::Record;
use crate::session::RunOutcome;
use crate::session::SessionId;
use crate::session::Turn;
use crate::session::TurnId;
use crate::session::content_digest;
use crate::tool::OnDangling;
use crate::tool::ToolDecl;
use crate::tool::ToolRegistry;

/// The cluster-wide receptionist key every node's host registers under
/// (harness spec §7.2).
pub fn host_key<S: HarnessSystem>() -> Key<Host<S>> {
    Key::new("harness.hosts")
}

// ---------------------------------------------------------------------------
// Kinds (§7.1)
// ---------------------------------------------------------------------------

/// A named agent definition (harness spec §2.2, §7.1): system prompt,
/// toolset, sandbox profile, model parameters, default budget, delegation
/// allowlist. Code-and-config, agreed cluster-wide like the codec (core spec
/// §5): every node MUST register every kind.
#[derive(Clone, Debug)]
pub struct Kind {
    pub system_prompt: String,
    pub params: ModelParams,
    pub tools: ToolRegistry,
    pub profile: SandboxProfile,
    pub default_budget: Budget,
    /// Child kinds this kind may delegate to (§8.1). Non-empty ⇒ the
    /// built-in `delegate` tool is in the model's toolset.
    pub delegates: Vec<KindId>,
}

impl Kind {
    /// Start a kind from its system prompt, with conservative defaults.
    pub fn new(system_prompt: impl Into<String>) -> Kind {
        Kind {
            system_prompt: system_prompt.into(),
            params: ModelParams::default(),
            tools: ToolRegistry::new(),
            profile: SandboxProfile::default(),
            default_budget: Budget::new(100_000, 25),
            delegates: Vec::new(),
        }
    }

    /// Set the model parameters.
    pub fn model(mut self, params: ModelParams) -> Kind {
        self.params = params;
        self
    }

    /// Declare a sandboxed tool (§5.2) at its required tier (§5.6), with the
    /// safe dangling policy (`Interrupt`, §5.5): on a crash-resume boundary
    /// the model, not the harness, decides whether to retry the side effect.
    /// An idempotent tool opts into blind re-execution via [`Kind::tool`].
    /// The tier is explicit because it is digest-covered deployment
    /// configuration (§7.1): visible at the declaration site, never defaulted.
    pub fn sandboxed(
        mut self,
        name: impl Into<String>,
        description: impl Into<String>,
        input_schema: &Value,
        tier: Tier,
    ) -> Kind {
        self.tools.declare(ToolDecl {
            name: name.into(),
            description: description.into(),
            input_schema: input_schema.clone(),
            tier,
            on_dangling: OnDangling::Interrupt,
            timeout: None,
        });
        self
    }

    /// Declare a sandboxed tool with full control over its declaration.
    pub fn tool(mut self, decl: ToolDecl) -> Kind {
        self.tools.declare(decl);
        self
    }

    /// Permit delegation to the named kinds (§8.1): the allowlist a locked
    /// down kind cannot escalate past (§5.2 — naming any other kind is a
    /// synthesized `ToolError`).
    pub fn delegates_to(mut self, kinds: &[&str]) -> Kind {
        self.delegates = kinds.iter().map(|k| KindId::new(*k)).collect();
        self
    }

    /// Set the sandbox profile (§5.3 item 4).
    pub fn sandbox(mut self, profile: SandboxProfile) -> Kind {
        self.profile = profile;
        self
    }

    /// Set the default run budget (§9.1).
    pub fn budget(mut self, budget: Budget) -> Kind {
        self.default_budget = budget;
        self
    }

    /// The effective tier cap (§5.3 item 4): the profile's explicit set, or
    /// the spec default — exactly the tiers the declared tools require.
    pub fn tier_cap(&self) -> BTreeSet<Tier> {
        self.profile
            .tier_cap
            .clone()
            .unwrap_or_else(|| self.tools.iter().map(|d| d.tier).collect())
    }

    /// A digest of the definition, pinned by `SessionCreated` (§7.1, §10.5)
    /// so a reader can tell whether a journal reconstruction is exact or
    /// merely indicative because a deployment changed the kind mid-session.
    /// Covers each tool's declared tier and the profile's effective cap
    /// (§5.6): what a session may acquire is cluster-wide agreement.
    ///
    /// The canonical form is length-prefixed (netstring-style) with a count
    /// before each variable-length list: no concatenation of two distinct
    /// definitions can collide, which bare juxtaposition cannot promise
    /// ("fo"+"obar" reads as "foo"+"bar").
    pub fn digest(&self) -> u64 {
        let mut canon = String::new();
        let mut frame = |field: &str| {
            canon.push_str(&field.len().to_string());
            canon.push(':');
            canon.push_str(field);
        };
        frame(&self.system_prompt);
        frame(&self.params.model);
        frame(&self.params.max_tokens.to_string());
        frame(&format!("tools={}", self.tools.iter().count()));
        for decl in self.tools.iter() {
            frame(&decl.name);
            frame(&decl.description);
            frame(&decl.input_schema.to_string());
            frame(&format!("{:?}", decl.tier));
            frame(&format!("{:?}", decl.on_dangling));
            frame(&format!("{:?}", decl.timeout));
        }
        frame(&self.profile.image);
        let cap = self.tier_cap();
        frame(&format!("cap={}", cap.len()));
        for tier in cap {
            frame(&format!("{tier:?}"));
        }
        frame(&format!("egress={}", self.profile.egress.len()));
        for host in &self.profile.egress {
            frame(host);
        }
        frame(&self.profile.compute.memory_bytes.to_string());
        frame(&self.profile.compute.fuel.to_string());
        frame(&self.default_budget.tokens.to_string());
        frame(&self.default_budget.steps.to_string());
        frame(&format!("delegates={}", self.delegates.len()));
        for kind in &self.delegates {
            frame(kind.as_str());
        }
        content_digest(&canon)
    }
}

/// The cluster-wide `KindId → Kind` map (harness spec §7.1), identical on
/// every node.
#[derive(Clone, Debug, Default)]
pub struct Kinds {
    map: BTreeMap<KindId, Arc<Kind>>,
}

impl Kinds {
    pub fn new() -> Kinds {
        Kinds::default()
    }

    /// Register a kind under its name. Builder-style, used at deployment
    /// configuration time.
    ///
    /// Panics when a declared tool's tier falls outside the kind's tier cap
    /// (§5.3 item 4): a deployment configuration error, surfaced here as
    /// loudly as a duplicate tool name — never discovered at dispatch. The
    /// loop performs no runtime cap check: the cap is unreachable by
    /// construction (§5.6, sandbox spec S4).
    pub fn register(mut self, name: &str, kind: Kind) -> Kinds {
        let cap = kind.tier_cap();
        for decl in kind.tools.iter() {
            assert!(
                cap.contains(&decl.tier),
                "kind '{name}': tool '{}' declares tier {:?} outside the tier cap {:?}",
                decl.name,
                decl.tier,
                cap
            );
        }
        self.map.insert(KindId::new(name), Arc::new(kind));
        self
    }

    /// The definition for `kind`, if this deployment registers it (§7.1).
    pub fn get(&self, kind: &KindId) -> Option<Arc<Kind>> {
        self.map.get(kind).cloned()
    }
}

// ---------------------------------------------------------------------------
// Wire contract (§7.3)
// ---------------------------------------------------------------------------

/// The second-hop handle a host returns instead of parking: the `ActorId` of
/// a per-call worker ([`TurnWaiter`] or [`TailReader`]) the client asks next.
/// `SessionRef` hides the hop (§7.4).
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Ticket {
    pub actor: ActorId,
}

/// A host's synchronous routing rejection — decisions a host makes from its
/// local view alone, with no I/O (§7.2).
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum HostReject {
    /// This host's view does not name its node owner (§7.2): fail fast; the
    /// `TurnId` makes the caller's retry safe once views converge.
    NotOwner,
    /// The named kind is not registered on this node — a deployment error
    /// (§7.1), never a silent fallback.
    UnknownKind(KindId),
    /// The session actor terminated while the op was being handed over (an
    /// activation boundary); retrying re-activates.
    Busy,
}

/// Submit a turn (§7.3): creation is implicit in a session's first turn, so
/// `kind` rides every call though it binds only on the first — after that it
/// is a checked redundancy, rejected on mismatch rather than ignored (§7.4).
/// `parent` is set by the delegation path (§8.1), `None` for user turns.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Submit {
    pub session: SessionId,
    pub kind: KindId,
    pub turn: Turn,
    pub parent: Option<Lineage>,
}

impl Message for Submit {
    type Reply = Result<Ticket, HostReject>;
    const MANIFEST: Manifest = Manifest::new("harness.Submit");
}

/// Cancel the run `turn` names (§7.3, §9.2). Idempotent.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Cancel {
    pub session: SessionId,
    pub turn: TurnId,
}

impl Message for Cancel {
    type Reply = Result<(), HostReject>;
    const MANIFEST: Manifest = Manifest::new("harness.Cancel");
}

/// Read committed records (§7.3, §10.2). Never activates the session.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Tail {
    pub session: SessionId,
    pub from: SeqNo,
    pub limit: u32,
}

impl Message for Tail {
    type Reply = Result<Ticket, HostReject>;
    const MANIFEST: Manifest = Manifest::new("harness.Tail");
}

/// Park on a turn's outcome (the second hop of `Submit`). `within_nanos`
/// bounds the wait — enforced by the waiter itself on the system clock, so
/// the ask is answered, never abandoned (core invariant #1).
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Await {
    pub within_nanos: u64,
}

impl Message for Await {
    type Reply = Awaited;
    const MANIFEST: Manifest = Manifest::new("harness.Await");
}

/// What the waiter observed.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub enum Awaited {
    /// The run's terminal outcome — recorded, or fresh off the live run.
    Outcome(RunOutcome),
    /// The submission was rejected without journaling anything (§7.4): a
    /// caller bug (content or kind mismatch) or a deployment error. Maps to
    /// `CallError::System`.
    Rejected(String),
    /// The wait deadline elapsed; the run continues (§7.3). Re-submitting
    /// the same `TurnId` re-attaches (H7).
    TimedOut,
    /// The activation went away before the outcome (deactivation, fence
    /// loss §6.2, restart): nothing recorded for the caller — re-submitting
    /// the same `TurnId` is the safe continuation.
    Lost,
}

/// Fetch the records a [`Tail`] ticket promised (the second hop).
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct TailFetch {}

impl Message for TailFetch {
    type Reply = Result<Vec<(SeqNo, Record)>, String>;
    const MANIFEST: Manifest = Manifest::new("harness.TailFetch");
}

// ---------------------------------------------------------------------------
// The host actor (§7.2)
// ---------------------------------------------------------------------------

/// The per-node actor that activates, routes to, and deactivates the
/// sessions its node owns (harness spec §7.2). Its handlers decide from the
/// local view and hand off; they perform no I/O and park on nothing, so one
/// slow session never stalls the node's routing.
pub struct Host<S: HarnessSystem> {
    harness: Harness<S>,
    /// The node's live activations. At most one entry per session, pruned by
    /// death watch — activations on one node are strictly sequential (the
    /// per-node half of H6).
    sessions: BTreeMap<SessionId, ActorRef<AgentActor<S>>>,
}

impl<S: HarnessSystem> Host<S> {
    pub(crate) fn new(harness: Harness<S>) -> Host<S> {
        Host {
            harness,
            sessions: BTreeMap::new(),
        }
    }

    /// Whether this node owns `session` per the local view (§7.2).
    fn owns(&self, session: &SessionId) -> bool {
        self.harness.system().owner_of(session.as_str().as_bytes())
            == Some(self.harness.system().node())
    }

    /// The live activation for `session`, activating lazily if absent
    /// (§7.2): the actor itself loads and folds the journal off the host's
    /// executor, so activation costs the host a spawn and nothing more.
    fn activation(&mut self, session: &SessionId, ctx: &Ctx<Self>) -> ActorRef<AgentActor<S>> {
        if let Some(actor) = self.sessions.get(session) {
            return actor.clone();
        }
        let harness = self.harness.clone();
        let id = session.clone();
        // Factory-spawned so supervision can restart it (§3.3): a restarted
        // instance reloads the journal — the same mechanism as a cross-node
        // resume, exercised locally.
        let actor = ctx.spawn_with(move || AgentActor::new(harness.clone(), id.clone()));
        ctx.watch(&actor);
        self.sessions.insert(session.clone(), actor.clone());
        actor
    }
}

impl<S: HarnessSystem> Actor for Host<S> {
    type System = S;

    // No background loop: the deactivation sweep (§7.2) runs per activation,
    // inside the agent (see `AgentActor`), so a node with no live session
    // schedules nothing — and a quiescence-driven simulation can actually
    // quiesce (core spec §18.1).

    fn register(registry: &mut HandlerRegistry<Self>) {
        registry.accept::<Submit>();
        registry.accept::<Cancel>();
        registry.accept::<Tail>();
    }
}

impl<S: HarnessSystem> Handler<Submit> for Host<S> {
    async fn handle(&mut self, msg: Submit, ctx: &Ctx<Self>) -> Result<Ticket, HostReject> {
        if !self.owns(&msg.session) {
            return Err(HostReject::NotOwner);
        }
        // A kind missing from this node is a deployment error that fails the
        // triggering call before any run starts (§7.1) — nothing journaled.
        let Some(kind_def) = self.harness.kinds().get(&msg.kind) else {
            return Err(HostReject::UnknownKind(msg.kind));
        };
        let agent = self.activation(&msg.session, ctx);
        let (tx, rx) = oneshot::channel();
        let op = SubmitOp {
            kind: msg.kind,
            kind_def,
            turn: msg.turn,
            parent: msg.parent,
            tx,
        };
        // Hand the op to the session actor on its own executor. `when_local`
        // is the sanctioned local channel (core spec §3.5.1) — and the only
        // one that can carry the non-serializable reply sender; host and
        // agent are co-located by construction.
        if agent
            .when_local(move |agent| agent.submit(op))
            .await
            .is_none()
        {
            return Err(HostReject::Busy);
        }
        let waiter = ctx.spawn(TurnWaiter::new(self.harness.clone(), rx));
        Ok(Ticket {
            actor: waiter.id().clone(),
        })
    }
}

impl<S: HarnessSystem> Handler<Cancel> for Host<S> {
    async fn handle(&mut self, msg: Cancel, ctx: &Ctx<Self>) -> Result<(), HostReject> {
        if !self.owns(&msg.session) {
            return Err(HostReject::NotOwner);
        }
        // A cancel is a resumption contact (§7.5): it activates the session,
        // which resumes any unfinished run before the cancel ends it.
        let agent = self.activation(&msg.session, ctx);
        let turn = msg.turn;
        if agent
            .when_local(move |agent| agent.cancel(turn))
            .await
            .is_none()
        {
            return Err(HostReject::Busy);
        }
        Ok(())
    }
}

impl<S: HarnessSystem> Handler<Tail> for Host<S> {
    async fn handle(&mut self, msg: Tail, ctx: &Ctx<Self>) -> Result<Ticket, HostReject> {
        if !self.owns(&msg.session) {
            return Err(HostReject::NotOwner);
        }
        // Served from the journal directly — a tail MUST NOT activate the
        // session (§10.2). The read parks the reader, never the host.
        let reader = ctx.spawn(TailReader {
            harness: self.harness.clone(),
            session: msg.session,
            from: msg.from,
            limit: msg.limit,
        });
        Ok(Ticket {
            actor: reader.id().clone(),
        })
    }
}

impl<S: HarnessSystem> Handler<Terminated> for Host<S> {
    async fn handle(&mut self, msg: Terminated, _ctx: &Ctx<Self>) {
        // An activation ended (deactivation, fence loss, supervision giving
        // up): prune so the next contact activates afresh — sequentially,
        // after this termination (the per-node half of H6).
        self.sessions.retain(|_, actor| actor.id() != &msg.id);
    }
}

// ---------------------------------------------------------------------------
// Per-call workers
// ---------------------------------------------------------------------------

/// Parks on one turn's outcome so nobody else has to (see the module note on
/// tickets). One waiter serves one `Submit` call and stops after answering.
pub struct TurnWaiter<S: HarnessSystem> {
    harness: Harness<S>,
    rx: Option<oneshot::Receiver<Awaited>>,
}

impl<S: HarnessSystem> TurnWaiter<S> {
    pub(crate) fn new(harness: Harness<S>, rx: oneshot::Receiver<Awaited>) -> TurnWaiter<S> {
        TurnWaiter {
            harness,
            rx: Some(rx),
        }
    }
}

impl<S: HarnessSystem> Actor for TurnWaiter<S> {
    type System = S;

    fn register(registry: &mut HandlerRegistry<Self>) {
        registry.accept::<Await>();
    }
}

impl<S: HarnessSystem> Handler<Await> for TurnWaiter<S> {
    async fn handle(&mut self, msg: Await, ctx: &Ctx<Self>) -> Awaited {
        ctx.stop();
        let Some(rx) = self.rx.take() else {
            // A second ask on a one-shot waiter: the first answer consumed
            // it. Treat as the attachment being gone; re-submitting attaches
            // afresh.
            return Awaited::Lost;
        };
        let within = Duration::from_nanos(msg.within_nanos);
        match self.harness.clock().timeout(within, rx).await {
            Ok(Ok(answer)) => answer,
            // The sending side was dropped without an answer: the activation
            // died or deactivated under us.
            Ok(Err(_cancelled)) => Awaited::Lost,
            Err(_elapsed) => Awaited::TimedOut,
        }
    }
}

/// Performs one `Tail` read on its own executor (§10.2) and stops.
pub struct TailReader<S: HarnessSystem> {
    harness: Harness<S>,
    session: SessionId,
    from: SeqNo,
    limit: u32,
}

impl<S: HarnessSystem> Actor for TailReader<S> {
    type System = S;

    fn register(registry: &mut HandlerRegistry<Self>) {
        registry.accept::<TailFetch>();
    }
}

impl<S: HarnessSystem> Handler<TailFetch> for TailReader<S> {
    async fn handle(
        &mut self,
        _msg: TailFetch,
        ctx: &Ctx<Self>,
    ) -> Result<Vec<(SeqNo, Record)>, String> {
        ctx.stop();
        self.harness
            .journal()
            .load(&self.session, self.from, self.limit as usize)
            .await
            .map_err(|e| e.to_string())
    }
}
