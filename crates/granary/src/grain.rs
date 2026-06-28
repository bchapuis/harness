//! The grain model: identity, behavior, the decide/apply split, and the dispatch
//! allowlist (spec §3, §4, §5.5).

use std::collections::BTreeSet;
use std::future::Future;
use std::marker::PhantomData;
use std::sync::Arc;

use actor_core::ActorRef;
use actor_core::BoxError;
use actor_core::HandlerRegistry;
use actor_core::Message;
use actor_serialization::SerializationRequirement;
use serde::Deserialize;
use serde::Serialize;

use crate::blobs::GrainBlobs;
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
    /// Build a name from its grain type and application key.
    pub fn new(grain_type: impl Into<String>, key: impl Into<String>) -> GrainName {
        GrainName {
            grain_type: grain_type.into(),
            key: key.into(),
        }
    }

    /// The grain type — the `GRAIN_TYPE` of the [`Grain`] implementation (§4).
    pub fn grain_type(&self) -> &str {
        &self.grain_type
    }

    /// The application key within the type's namespace.
    pub fn key(&self) -> &str {
        &self.key
    }
}

impl std::fmt::Display for GrainName {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}/{}", self.grain_type, self.key)
    }
}

/// A virtual, durable, single-activation object (spec §4.1).
///
/// The author implements the **behavior** (immutable configuration) as a type,
/// declares the **state** and **event** types, and writes the pure fold
/// [`apply`](Grain::apply). The runtime supplies identity, durability, the gates,
/// and the lifecycle.
///
/// `Self::System` must be a [`GranarySystem`] — a system that can host grains
/// (`Local` [`LocalSystem`](actor_core::LocalSystem), or a `Quorum` shard-hosting
/// clustered system). This refines the spec's `System: ActorSystem` with the
/// capabilities a grain activation needs (timer, task launch, event stream).
pub trait Grain: Sized + Send + 'static {
    /// The system this grain's activation runs on.
    type System: GranarySystem;

    /// The folded state and snapshot payload. Rebuilt from the journal on
    /// activation; `Default` is the empty state at `Seq::ZERO`.
    type State: SerializationRequirement + Default;

    /// The journal record type: the unit of durable change.
    type Event: SerializationRequirement;

    /// The grain type's stable identity — the namespace tag in every
    /// [`GrainName`] of this type and the key the gateway is discovered under
    /// (§5.3). An explicit constant (e.g. `"bank.Account"`) is RECOMMENDED.
    ///
    /// (An addition beyond the §4.1 trait sketch: the runtime needs a stable,
    /// serializable type tag, and deriving one from `type_name` would not be
    /// rename-stable.)
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

    /// Called once before the activation deactivates — idle eviction or handoff
    /// (§10).
    fn on_passivate(&mut self, _ctx: &GrainCtx<Self>) -> impl Future<Output = ()> + Send {
        async {}
    }

    /// Whether the activation MAY idle-hibernate now (§10). Consulted only on idle
    /// eviction; a forced step-down (leadership move, quorum loss) is involuntary
    /// and ignores it. The default always permits hibernation — a reactive grain
    /// keeps nothing in flight that a replay would not reconstruct. A grain with
    /// **autonomous** work that is not yet journaled (the agentic harness's live
    /// run, harness §7.2) overrides this to veto eviction until the work settles;
    /// the host reschedules the idle check rather than evicting.
    fn can_passivate(&self, _state: &Self::State) -> bool {
        true
    }
}

/// A grain's handler for one command type (spec §4.2): the **decide** half of the
/// decide/apply split.
///
/// `handle` inspects the current state and returns the events to persist together
/// with the reply. It is a *decision*, not a mutation: it MUST NOT mutate state
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

/// The handler/lifecycle context (spec §4.3). Exposes the grain's name, a
/// self-reference, and the system handle. It deliberately exposes **no**
/// `persist` method and no state mutation — state changes only through events
/// folded by [`Grain::apply`] (§4.2).
pub struct GrainCtx<G: Grain> {
    grain_type: &'static str,
    name: GrainName,
    system: G::System,
    gateway: ActorRef<Gateway<G>>,
    /// The journal seam, so the grain can reach its colocated content-addressed blob
    /// area ([`blobs`](GrainCtx::blobs)). The journal routes to the grain's shard
    /// replicas, the same ones its records live on.
    journal: Arc<dyn DynGrainJournal>,
}

impl<G: Grain> GrainCtx<G> {
    pub(crate) fn new(
        grain_type: &'static str,
        name: GrainName,
        system: G::System,
        gateway: ActorRef<Gateway<G>>,
        journal: Arc<dyn DynGrainJournal>,
    ) -> GrainCtx<G> {
        GrainCtx {
            grain_type,
            name,
            system,
            gateway,
            journal,
        }
    }

    /// This grain's name.
    pub fn name(&self) -> &GrainName {
        &self.name
    }

    /// A handle to this grain's **colocated content-addressed blob area**
    /// (durable-workspace design): immutable bulk bytes the grain stores by content
    /// and references by [`BlobId`](crate::BlobId) from its small foldable state. The
    /// blobs ride the grain's own shard replicas, so a read is local in steady state;
    /// the grain drives their reclamation from its own live id set
    /// ([`GrainBlobs::gc`](crate::GrainBlobs::gc)). This is the grain analogue of a
    /// Durable Object's colocated storage (DO §2.3) — beside, not in, the journal.
    pub fn blobs(&self) -> GrainBlobs {
        GrainBlobs::new(Arc::clone(&self.journal), self.name.clone())
    }

    /// A shareable self-reference (spec §4.3). It resolves through the gateway each
    /// call (no host cache): a self-reference is used rarely, not on a hot path.
    pub fn this(&self) -> GrainRef<G> {
        GrainRef::new(
            self.grain_type,
            self.name.clone(),
            self.gateway.clone(),
            self.system.clone(),
            None,
        )
    }

    /// The system this activation runs on.
    pub fn system(&self) -> &G::System {
        &self.system
    }
}

/// Register `RunTyped<M>` on the host (spec §5.5): a free fn (no captured state)
/// so it can be stored as a plain `fn` pointer in [`GrainRegistry`], the same
/// no-codegen registration primitive the actor `HandlerRegistry` uses. Bridges
/// `Grain::register` (which names the commands) to `Host::register` (which must
/// accept them over the network).
fn register_run_typed<G, M>(registry: &mut HandlerRegistry<Host<G>>)
where
    G: GrainHandler<M>,
    M: Message,
{
    registry.accept::<RunTyped<M>>();
}

/// Maps the commands a grain accepts to its deserialization allowlist (spec
/// §5.5), the grain analogue of `HandlerRegistry`. `Grain::register` fills it via
/// `r.accept::<M>()`.
///
/// It records two things per `accept::<M>()`: the manifest (the allowlist, read by
/// [`accepted_manifests`]) and a dispatch-registration thunk that teaches the
/// [`Host`] to accept `RunTyped<M>` over the network ([`Host::register`] replays
/// these). The over-the-wire dispatch is therefore the actor framework's own
/// registry — granary adds no transport. A name-addressed command whose manifest
/// is unregistered is `GrainError::Unhandled`.
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

    /// Accept command type `M` (spec §5.5). The `G: GrainHandler<M>` bound proves
    /// the grain actually handles `M`.
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

    /// The host-registration thunks, one per accepted command, replayed by
    /// [`Host::register`] to build the host's network dispatch table.
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
