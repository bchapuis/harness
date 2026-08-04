//! The grain model: identity, behavior, the decide/apply split, and the dispatch
//! allowlist (spec §3, §4, §5.5).

use std::collections::BTreeSet;
use std::future::Future;
use std::marker::PhantomData;
use std::sync::Arc;

use actor_core::ActorId;
use actor_core::ActorRef;
use actor_core::BoxError;
use actor_core::HandlerRegistry;
use actor_core::Message;
use actor_core::TerminationReason;
use actor_serialization::SerializationRequirement;
use serde::Deserialize;
use serde::Serialize;

use crate::blobs::GrainBlobs;
use crate::facet::FacetCell;
use crate::facet::FacetSet;
use crate::gateway::Gateway;
use crate::grainref::GrainRef;
use crate::host::Host;
use crate::host::RunTyped;
use crate::journal::DynGrainJournal;
use crate::system::GranarySystem;

/// The stable, cluster-wide identity of a grain (spec §3): a `(grain type, key)`
/// pair, where `key` is an arbitrary application string (`"account/42"`, a UUID,
/// a tenant id). Unlike an `ActorId`, a `GrainName` names a logical object, not a
/// node — it is not locality-classifiable on its own (§5.1).
#[derive(Clone, PartialEq, Eq, Hash, PartialOrd, Ord, Debug, Serialize, Deserialize)]
pub struct GrainName {
    grain_type: String,
    key: String,
}

impl GrainName {
    pub fn new(grain_type: impl Into<String>, key: impl Into<String>) -> GrainName {
        GrainName {
            grain_type: grain_type.into(),
            key: key.into(),
        }
    }

    /// The `GRAIN_TYPE` of the [`Grain`] implementation (§4).
    pub fn grain_type(&self) -> &str {
        &self.grain_type
    }

    pub fn key(&self) -> &str {
        &self.key
    }
}

impl std::fmt::Display for GrainName {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}/{}", self.grain_type, self.key)
    }
}

/// The uninhabited event type for a grain whose durable state lives entirely in
/// its facets (spec §7.12): `type Event = NoEvent` declares that facet 0 journals
/// nothing, and `apply` is the empty match. The workspace grain (§7.11) is the
/// canonical user.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum NoEvent {}

impl NoEvent {
    /// The `apply` body for a facet-only grain: a `NoEvent` cannot exist.
    pub fn unreachable(&self) -> ! {
        match *self {}
    }
}

/// A virtual, durable, single-activation object (spec §4.1).
///
/// The implementing type is the **behavior**: immutable configuration. The
/// runtime supplies identity, durability, the gates, and the lifecycle.
///
/// `Self::System` must be a [`GranarySystem`] — a system that can host grains
/// (`Local` [`LocalSystem`](actor_core::LocalSystem), or a `Quorum` shard-hosting
/// clustered system).
pub trait Grain: Sized + Send + 'static {
    type System: GranarySystem;

    /// The folded state and snapshot payload. Rebuilt from the journal on
    /// activation; `Default` is the empty state at `Seq::ZERO`.
    type State: SerializationRequirement + Default;

    /// The journal record type: the unit of durable change.
    type Event: SerializationRequirement;

    /// The grain's declared facet set (spec §7.12): `()` for none, or a tuple of
    /// built-in facets — e.g. `(Kv,)`, `(Kv, Ws)`. Each declared facet
    /// surfaces a compile-time-gated [`GrainCtx`] accessor, contributes tagged
    /// records to the same atomic per-command batch (G19), joins the composite
    /// snapshot, and adds its blob roots to the grain's unioned live set.
    type Facets: FacetSet;

    /// The grain type's stable identity — the namespace tag in every
    /// [`GrainName`] of this type and the key the gateway is discovered under
    /// (§5.3). An explicit constant (e.g. `"bank.Account"`) is RECOMMENDED:
    /// deriving one from `type_name` would not be rename-stable.
    const GRAIN_TYPE: &'static str;

    /// Apply one event to state (spec §4.1). MUST be pure and deterministic: it
    /// runs on the live commit path AND on replay/rehydration, and the two MUST
    /// agree (invariant **G2**). It MUST NOT perform I/O, read the clock, or use
    /// entropy.
    fn apply(state: &mut Self::State, event: &Self::Event);

    /// List the command messages this grain accepts over the network (§5.5).
    /// Mirrors `Actor::register`; the default registers nothing (a grain reached
    /// only locally, as in the `Local` tier).
    fn register(_registry: &mut GrainRegistry<Self>) {}

    /// Called once after the activation has rehydrated, before the first command
    /// (§10). Returning `Err` aborts activation.
    fn on_activate(
        &mut self,
        _ctx: &GrainCtx<Self>,
    ) -> impl Future<Output = Result<(), BoxError>> + Send {
        async { Ok(()) }
    }

    /// Called once before the activation deactivates — idle eviction, handoff, or
    /// a forced step-down (§10). Safe even when the journal is unwritable, because
    /// it cannot persist ([`GrainCtx`] exposes no `persist`); use it to release
    /// non-durable per-activation resources.
    fn on_passivate(&mut self, _ctx: &GrainCtx<Self>) -> impl Future<Output = ()> + Send {
        async {}
    }

    /// Whether the activation MAY idle-hibernate now (§10). Consulted only on idle
    /// eviction; a forced step-down (leadership move, quorum loss) is involuntary
    /// and ignores it. The default always permits hibernation. A grain with
    /// **autonomous** work that is not yet journaled (harness §7.2) overrides this
    /// to veto eviction until the work settles; the host then reschedules the idle
    /// check rather than evicting.
    fn can_passivate(&self, _state: &Self::State) -> bool {
        true
    }

    /// Called when the grain's durable alarm fires (spec §7.16), with no caller
    /// present. Like a [`GrainHandler`] minus the reply: a **decision** returning
    /// the events to journal, and it MAY re-arm or cancel the alarm through
    /// [`ctx.alarm()`](GrainCtx::alarm) (staged into the same atomic batch).
    /// Delivered only while a deadline armed through the [`Alarm`] facet is due.
    /// Same durability barrier as a command (§6): its events and staged alarm
    /// change commit before any effect they imply.
    ///
    /// [`Alarm`]: crate::Alarm
    fn on_alarm(
        &self,
        _state: &Self::State,
        _ctx: &GrainCtx<Self>,
    ) -> impl Future<Output = Vec<Self::Event>> + Send {
        async { Vec::new() }
    }

    /// Called when an actor this grain watched through
    /// [`GrainCtx::watch`] terminates (actor §12) — including its node going
    /// down. Like [`on_alarm`](Grain::on_alarm), a callerless **decision**
    /// through the same §6 barrier: return the events to journal (an empty
    /// result commits nothing, §7.5). The default ignores the signal.
    fn on_peer_terminated(
        &self,
        _state: &Self::State,
        _ctx: &GrainCtx<Self>,
        _peer: &ActorId,
        _reason: TerminationReason,
    ) -> impl Future<Output = Vec<Self::Event>> + Send {
        async { Vec::new() }
    }
}

/// A grain's handler for one command type (spec §4.2): the **decide** half of the
/// decide/apply split.
///
/// `handle` is a *decision*, not a mutation: it MUST NOT mutate state
/// directly (state changes only through [`Grain::apply`]) and MUST NOT perform
/// durable I/O (the host owns persistence, §6). A read-only command returns no
/// events — `(vec![], reply)` — which commits nothing (§7.5).
pub trait GrainHandler<M: Message>: Grain {
    /// Decide the outcome of a command (spec §4.2).
    fn handle(
        &self,
        state: &Self::State,
        msg: M,
        ctx: &GrainCtx<Self>,
    ) -> impl Future<Output = (Vec<Self::Event>, M::Reply)> + Send;
}

/// The handler/lifecycle context (spec §4.3). It deliberately exposes **no**
/// `persist` method and no state mutation — state changes only through events
/// folded by [`Grain::apply`] (§4.2).
pub struct GrainCtx<G: Grain> {
    grain_type: &'static str,
    name: GrainName,
    system: G::System,
    gateway: ActorRef<Gateway<G>>,
    /// The journal seam, so the grain can reach its colocated blob area
    /// ([`blobs`](GrainCtx::blobs)).
    journal: Arc<dyn DynGrainJournal>,
    /// The host's facet cell (spec §7.12): committed forms plus the per-command
    /// stage, shared so the facet accessors (`kv()`, `ws()`, …) read and stage
    /// through it.
    facets: Arc<FacetCell<G::Facets>>,
    watches: Arc<std::sync::Mutex<Vec<ActorId>>>,
    /// This node's blocking-I/O pool (§7.4). Physical facets reach it to keep their
    /// scans off the async executor: a facet that reads and hashes a whole image
    /// inline stalls the worker driving this node's Raft heartbeats, which is a
    /// cluster-wide event produced by one grain's local work.
    blocking_io: Arc<dyn crate::BlockingIo>,
}

impl<G: Grain> GrainCtx<G> {
    #[allow(clippy::too_many_arguments)] // one call site, `Host::grain_ctx`
    pub(crate) fn new(
        grain_type: &'static str,
        name: GrainName,
        system: G::System,
        gateway: ActorRef<Gateway<G>>,
        journal: Arc<dyn DynGrainJournal>,
        facets: Arc<FacetCell<G::Facets>>,
        watches: Arc<std::sync::Mutex<Vec<ActorId>>>,
        blocking_io: Arc<dyn crate::BlockingIo>,
    ) -> GrainCtx<G> {
        GrainCtx {
            grain_type,
            name,
            system,
            gateway,
            journal,
            facets,
            watches,
            blocking_io,
        }
    }

    /// Death-watch `target` (actor §12): when it terminates for any reason —
    /// including its node going down — the grain's
    /// [`on_peer_terminated`](Grain::on_peer_terminated) runs through the
    /// ordinary §6 barrier. The registration is queued here and installed by the
    /// host after the current handler (or lifecycle hook) completes; it lives
    /// with the activation, so a rehydrated grain re-watches from its folded
    /// state in `on_activate` (watch-after-death fires immediately, actor §12).
    pub fn watch(&self, target: ActorId) {
        self.watches.lock().expect("watch queue lock").push(target);
    }

    pub(crate) fn facet_cell(&self) -> &Arc<FacetCell<G::Facets>> {
        &self.facets
    }

    /// This node's blocking-I/O pool (spec §7.4).
    ///
    /// For a **physical facet** whose work is bulk device I/O and hashing rather than
    /// coordination: the disk facet's capture scans and hashes a whole image, which on
    /// the async worker stalls this node's Raft heartbeats for as long as it runs. The
    /// default is [`InlineIo`](crate::InlineIo), so a deployment that has not opted
    /// into a pool — and the deterministic simulation, which must not — behaves exactly
    /// as it did before the seam existed.
    pub(crate) fn blocking_io(&self) -> &Arc<dyn crate::BlockingIo> {
        &self.blocking_io
    }

    pub fn name(&self) -> &GrainName {
        &self.name
    }

    /// A handle to this grain's **colocated content-addressed blob area**:
    /// immutable bulk bytes the grain stores by content and references by
    /// [`BlobId`](crate::BlobId) from its small foldable state. They sit beside,
    /// not in, the journal, on the grain's own shard replicas, and the grain
    /// drives their reclamation from its own live id set
    /// ([`GrainBlobs::gc`](crate::GrainBlobs::gc)).
    pub fn blobs(&self) -> GrainBlobs {
        // Every sweep through this handle unions the facets' live roots (spec
        // §7.12), so neither the grain nor the host can drop the other's bytes.
        let cell = Arc::clone(&self.facets);
        GrainBlobs::new(Arc::clone(&self.journal), self.name.clone())
            .with_facet_roots(Arc::new(move || cell.roots()))
    }

    /// A shareable self-reference (spec §4.3). It resolves through the gateway on
    /// each call, with no host cache.
    pub fn this(&self) -> GrainRef<G> {
        GrainRef::new(
            self.grain_type,
            self.name.clone(),
            self.gateway.clone(),
            self.system.clone(),
            None,
        )
    }

    pub fn system(&self) -> &G::System {
        &self.system
    }
}

/// Register `RunTyped<M>` on the host (spec §5.5): a free fn (no captured state)
/// so it can be stored as a plain `fn` pointer in [`GrainRegistry`].
fn register_run_typed<G, M>(registry: &mut HandlerRegistry<Host<G>>)
where
    G: GrainHandler<M>,
    M: Message,
{
    registry.accept::<RunTyped<M>>();
}

/// Maps the commands a grain accepts to its deserialization allowlist (spec
/// §5.5), the grain analogue of `HandlerRegistry`. `Grain::register` fills it via
/// `r.accept::<M>()`. A name-addressed command whose manifest is unregistered
/// surfaces from the transport as `GrainError::Call(CallError::Unhandled)`.
pub struct GrainRegistry<G: Grain> {
    accepted: BTreeSet<&'static str>,
    host_entries: Vec<fn(&mut HandlerRegistry<Host<G>>)>,
    _marker: PhantomData<fn() -> G>,
}

impl<G: Grain> GrainRegistry<G> {
    pub(crate) fn new() -> GrainRegistry<G> {
        GrainRegistry {
            accepted: BTreeSet::new(),
            host_entries: Vec::new(),
            _marker: PhantomData,
        }
    }

    /// Accept command type `M` (spec §5.5).
    pub fn accept<M>(&mut self)
    where
        G: GrainHandler<M>,
        M: Message,
    {
        self.accepted.insert(M::MANIFEST.as_str());
        self.host_entries.push(register_run_typed::<G, M>);
    }

    /// The manifests this grain accepts, in deterministic order.
    pub fn accepted(&self) -> &BTreeSet<&'static str> {
        &self.accepted
    }

    /// The host-registration thunks, replayed by [`Host::register`] to build the
    /// host's network dispatch table.
    pub(crate) fn host_entries(&self) -> &[fn(&mut HandlerRegistry<Host<G>>)] {
        &self.host_entries
    }
}

/// The set of command manifests a grain type accepts (spec §5.5) — the
/// deserialization allowlist, obtained by running [`Grain::register`].
pub fn accepted_manifests<G: Grain>() -> BTreeSet<&'static str> {
    let mut registry = GrainRegistry::<G>::new();
    G::register(&mut registry);
    registry.accepted
}
