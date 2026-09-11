//! The entity model: identity, behavior, the decide/apply split, and the dispatch
//! allowlist (spec §3, §4, §5.5).

use std::collections::BTreeSet;
use std::future::Future;
use std::marker::PhantomData;
use std::sync::Arc;

use actor_core::ActorId;
use actor_core::ActorRef;
use actor_core::ActorSystem;
use actor_core::BoxError;
use actor_core::BoxFuture;
use actor_core::HandlerRegistry;
use actor_core::Message;
use actor_core::TerminationReason;
use actor_serialization::SerializationRequirement;
use serde::Deserialize;
use serde::Serialize;

use crate::blobs::EntityBlobs;
use crate::entity_ref::EntityRef;
use crate::facet::EVENT_TAG;
use crate::facet::FacetCell;
use crate::facet::FacetSet;
use crate::facet::split_record;
use crate::gateway::Gateway;
use crate::host::Host;
use crate::host::RunTyped;
use crate::journal::DynEntityJournal;
use crate::journal::EntityJournalError;
use crate::journal::Seq;
use crate::system::EntitySystem;

/// The stable, cluster-wide identity of an entity (spec §3): a `(entity type, key)`
/// pair, where `key` is an arbitrary application string (`"account/42"`, a UUID,
/// a tenant id). Unlike an `ActorId`, an `EntityName` names a logical object, not a
/// node: it is not locality-classifiable on its own (§5.1).
#[derive(Clone, PartialEq, Eq, Hash, PartialOrd, Ord, Debug, Serialize, Deserialize)]
pub struct EntityName {
    entity_type: String,
    key: String,
}

impl EntityName {
    pub fn new(entity_type: impl Into<String>, key: impl Into<String>) -> EntityName {
        EntityName {
            entity_type: entity_type.into(),
            key: key.into(),
        }
    }

    /// The `ENTITY_TYPE` of the [`Entity`] implementation (§4).
    pub fn entity_type(&self) -> &str {
        &self.entity_type
    }

    pub fn key(&self) -> &str {
        &self.key
    }
}

impl std::fmt::Display for EntityName {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}/{}", self.entity_type, self.key)
    }
}

/// The uninhabited event type for an entity whose durable state lives entirely in
/// its facets (spec §7.12): `type Event = NoEvent` declares that facet 0 journals
/// nothing, and `apply` is the empty match. The workspace entity (§7.11) is the
/// canonical user.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum NoEvent {}

impl NoEvent {
    /// The `apply` body for a facet-only entity: a `NoEvent` cannot exist.
    pub fn unreachable(&self) -> ! {
        match *self {}
    }
}

/// A virtual, durable, single-activation object (spec §4.1).
///
/// The implementing type is the **behavior**: immutable configuration. The
/// runtime supplies identity, durability, the gates, and the lifecycle.
///
/// `Self::System` must be an [`EntitySystem`]: a system that can host entities
/// (`Local` [`LocalSystem`](actor_core::LocalSystem), or a `Quorum` shard-hosting
/// clustered system).
pub trait Entity: Sized + Send + 'static {
    type System: EntitySystem;

    /// The folded state and snapshot payload. Rebuilt from the journal on
    /// activation; `Default` is the empty state at `Seq::ZERO`.
    type State: SerializationRequirement + Default;

    /// The journal record type: the unit of durable change.
    type Event: SerializationRequirement;

    /// The entity's declared facet set (spec §7.12): `()` for none, or a tuple of
    /// built-in facets, e.g. `(Kv,)`, `(Kv, Ws)`. Each declared facet
    /// surfaces a compile-time-gated [`EntityCtx`] accessor, contributes tagged
    /// records to the same atomic per-command batch (E19), joins the composite
    /// snapshot, and adds its blob roots to the entity's unioned live set.
    type Facets: FacetSet;

    /// The entity type's stable identity: the namespace tag in every
    /// [`EntityName`] of this type and the key the gateway is discovered under
    /// (§5.3). An explicit constant (e.g. `"bank.Account"`) is RECOMMENDED:
    /// deriving one from `type_name` would not be rename-stable.
    const ENTITY_TYPE: &'static str;

    /// Apply one event to state (spec §4.1). MUST be pure and deterministic: it
    /// runs on the live commit path AND on replay/rehydration, and the two MUST
    /// agree (invariant **E2**). It MUST NOT perform I/O, read the clock, or use
    /// entropy.
    fn apply(state: &mut Self::State, event: &Self::Event);

    /// List the command messages this entity accepts over the network (§5.5).
    /// Mirrors `Actor::register`; the default registers nothing (an entity reached
    /// only locally, as in the `Local` tier).
    fn register(_registry: &mut EntityRegistry<Self>) {}

    /// Called once after the activation has rehydrated, before the first command
    /// (§10). Returning `Err` aborts activation.
    fn on_activate(
        &mut self,
        _ctx: &EntityCtx<Self>,
    ) -> impl Future<Output = Result<(), BoxError>> + Send {
        async { Ok(()) }
    }

    /// Called once before the activation deactivates: idle eviction, handoff, or
    /// a forced step-down (§10). Safe even when the journal is unwritable, because
    /// it cannot persist ([`EntityCtx`] exposes no `persist`); use it to release
    /// non-durable per-activation resources.
    fn on_deactivate(&mut self, _ctx: &EntityCtx<Self>) -> impl Future<Output = ()> + Send {
        async {}
    }

    /// Whether the activation MAY idle-hibernate now (§10). Consulted only on idle
    /// eviction; a forced step-down (leadership move, quorum loss) is involuntary
    /// and ignores it. The default always permits hibernation. An entity with
    /// **autonomous** work that is not yet journaled (agent §7.2) overrides this
    /// to veto eviction until the work settles; the host then reschedules the idle
    /// check rather than evicting.
    fn can_hibernate(&self, _state: &Self::State) -> bool {
        true
    }

    /// Called when the entity's durable alarm fires (spec §7.16), with no caller
    /// present. Like an [`EntityHandler`] minus the reply: a **decision** returning
    /// the events to journal, and it MAY re-arm or cancel the alarm through
    /// [`ctx.alarm()`](EntityCtx::alarm) (staged into the same atomic batch).
    /// Delivered only while a deadline armed through the [`Alarm`] facet is due.
    /// Same durability barrier as a command (§6): its events and staged alarm
    /// change commit before any effect they imply.
    ///
    /// [`Alarm`]: crate::alarm::Alarm
    fn on_alarm(
        &self,
        _state: &Self::State,
        _ctx: &EntityCtx<Self>,
    ) -> impl Future<Output = Vec<Self::Event>> + Send {
        async { Vec::new() }
    }

    /// Called when an actor this entity watched through
    /// [`EntityCtx::watch`] terminates (actor §12), including its node going
    /// down. Like [`on_alarm`](Entity::on_alarm), a callerless **decision**
    /// through the same §6 barrier: return the events to journal (an empty
    /// result commits nothing, §7.5). The default ignores the signal.
    fn on_peer_terminated(
        &self,
        _state: &Self::State,
        _ctx: &EntityCtx<Self>,
        _peer: &ActorId,
        _reason: TerminationReason,
    ) -> impl Future<Output = Vec<Self::Event>> + Send {
        async { Vec::new() }
    }
}

/// An entity's handler for one command type (spec §4.2): the **decide** half of the
/// decide/apply split.
///
/// `handle` is a *decision*, not a mutation: it MUST NOT mutate state
/// directly (state changes only through [`Entity::apply`]) and MUST NOT perform
/// durable I/O (the host owns persistence, §6). A read-only command returns no
/// events: `(vec![], reply)`, which commits nothing (§7.5).
pub trait EntityHandler<M: Message>: Entity {
    /// Decide the outcome of a command (spec §4.2).
    fn handle(
        &self,
        state: &Self::State,
        msg: M,
        ctx: &EntityCtx<Self>,
    ) -> impl Future<Output = (Vec<Self::Event>, M::Reply)> + Send;
}

/// A completed event read ([`EntityCtx::events`], [`EntityRef::events`]
/// (crate::EntityRef::events)): the decoded events, plus the highest compaction
/// **base** the read observed (§9). A slot at `Seq <= base` that is absent from
/// `events` is truncated history (compacted away, subsumed by the entity's
/// snapshot), not one of the interleaved records the projection skips (§7.12).
/// Every page carries the base because compaction is not bounded by any reader's
/// cursor (a snapshot at the head can outrun a slow pager), so a multi-page read
/// reports the highest it saw.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EventHistory<E> {
    /// The compaction base: absent slots at or below it are truncation, and
    /// `base > from` means the read's own history starts truncated.
    pub base: Seq,
    /// The committed events after `from`, decoded, at the journal's real slots.
    pub events: Vec<(Seq, E)>,
}

/// The boxed result of [`EntityCtx::events`]: the entity's committed events after
/// a `Seq` ([`EventHistory`]), decoded, at the journal's real slots.
pub type EventsFuture<E> = BoxFuture<'static, Result<EventHistory<E>, EntityJournalError>>;

/// One loaded journal page clamped to the committed prefix (§7.5), with the
/// at-head verdict derived from the clamp: slots above `floor` are an
/// in-flight append's tentative writes (or undecided leftovers), served only
/// once a commit or recovery raises the floor, and a page is at head exactly
/// when the clamp cut it short or the store ran out before `limit`. Returns
/// `(base, committed records, at_head)`. Shared by the activation's
/// [`EntityCtx::events`] and the gateway's non-activating read, so the two
/// cannot drift.
pub(crate) fn committed_prefix(
    page: crate::journal::RecordPage,
    floor: Seq,
    limit: usize,
) -> (Seq, Vec<(Seq, Vec<u8>)>, bool) {
    let base = page.base;
    let loaded = page.records.len();
    let records: Vec<(Seq, Vec<u8>)> = page
        .records
        .into_iter()
        .take_while(|(seq, _)| *seq <= floor)
        .collect();
    let at_head = records.len() < loaded || loaded < limit;
    (base, records, at_head)
}

/// The events among one loaded journal page, the record-to-event projection
/// (spec §7.9) applied to a `load` page: each record split into facet tag and
/// payload, facet records (§7.12) skipped, the events returned at the journal's
/// real slots. Shared by the activation's [`EntityCtx::events`] and the gateway's
/// non-activating read (§7.5), so the two projections cannot drift. A record
/// that will not split is corruption: an error, never silently skipped.
/// Consumes the page and strips each event's one-byte envelope in place, so
/// the serial gateway pays a memmove per event, never a second allocation.
pub(crate) fn events_in_page(
    page: Vec<(Seq, Vec<u8>)>,
) -> Result<Vec<(Seq, Vec<u8>)>, EntityJournalError> {
    let mut events = Vec::new();
    for (seq, mut bytes) in page {
        let (tag, _) =
            split_record(&bytes).map_err(|e| EntityJournalError::Unavailable(e.to_string()))?;
        if tag == EVENT_TAG {
            bytes.drain(..1);
            events.push((seq, bytes));
        }
    }
    Ok(events)
}

/// The handler/lifecycle context (spec §4.3). It deliberately exposes **no**
/// `persist` method and no state mutation: state changes only through events
/// folded by [`Entity::apply`] (§4.2).
pub struct EntityCtx<G: Entity> {
    entity_type: &'static str,
    name: EntityName,
    system: G::System,
    gateway: ActorRef<Gateway<G>>,
    /// The journal seam, so the entity can reach its colocated blob area
    /// ([`blobs`](EntityCtx::blobs)).
    journal: Arc<dyn DynEntityJournal>,
    /// The host's published committed head (spec §7.5): the floor of
    /// [`events`](EntityCtx::events). The local store alone cannot tell a
    /// committed record from a tentative one (a leader writes its own replica
    /// before the quorum resolves, §7.2), so the read clamps to this cell
    /// rather than trusting the input gate.
    committed: crate::host::CommittedCell,
    /// The host's facet cell (spec §7.12): committed forms plus the per-command
    /// stage, shared so the facet accessors (`kv()`, `ws()`, …) read and stage
    /// through it.
    facets: Arc<FacetCell<G::Facets>>,
    watches: Arc<std::sync::Mutex<Vec<ActorId>>>,
    /// This node's blocking-I/O pool (§7.4). Physical facets reach it to keep their
    /// scans off the async executor: a facet that reads and hashes a whole image
    /// inline stalls the worker driving this node's Raft heartbeats, which is a
    /// cluster-wide event produced by one entity's local work.
    blocking_io: Arc<dyn crate::blocking::BlockingIo>,
}

impl<G: Entity> EntityCtx<G> {
    #[allow(clippy::too_many_arguments)] // one call site, `Host::entity_ctx`
    pub(crate) fn new(
        entity_type: &'static str,
        name: EntityName,
        system: G::System,
        gateway: ActorRef<Gateway<G>>,
        journal: Arc<dyn DynEntityJournal>,
        committed: crate::host::CommittedCell,
        facets: Arc<FacetCell<G::Facets>>,
        watches: Arc<std::sync::Mutex<Vec<ActorId>>>,
        blocking_io: Arc<dyn crate::blocking::BlockingIo>,
    ) -> EntityCtx<G> {
        EntityCtx {
            entity_type,
            name,
            system,
            gateway,
            journal,
            committed,
            facets,
            watches,
            blocking_io,
        }
    }

    /// Death-watch `target` (actor §12): when it terminates for any reason
    /// (including its node going down) the entity's
    /// [`on_peer_terminated`](Entity::on_peer_terminated) runs through the
    /// ordinary §6 barrier. The registration is queued here and installed by the
    /// host after the current handler (or lifecycle hook) completes; it lives
    /// with the activation, so a rehydrated entity re-watches from its folded
    /// state in `on_activate` (watch-after-death fires immediately, actor §12).
    pub fn watch(&self, target: ActorId) {
        self.watches.lock().expect("watch queue lock").push(target);
    }

    pub(crate) fn facet_cell(&self) -> &Arc<FacetCell<G::Facets>> {
        &self.facets
    }

    /// This node's blocking-I/O pool (spec §7.4), for a **physical facet** whose work
    /// is bulk device I/O and hashing rather than coordination: the disk facet's
    /// capture scans and hashes a whole image, which on the async worker stalls this
    /// node's Raft heartbeats for as long as it runs. The default is
    /// [`InlineIo`](crate::blocking::InlineIo), which runs the work inline: what a
    /// deployment that has not opted into a pool gets, and what the deterministic
    /// simulation requires.
    pub(crate) fn blocking_io(&self) -> &Arc<dyn crate::blocking::BlockingIo> {
        &self.blocking_io
    }

    pub fn name(&self) -> &EntityName {
        &self.name
    }

    /// A handle to this entity's **colocated content-addressed blob area**:
    /// immutable bulk bytes the entity stores by content and references by
    /// [`BlobId`](crate::blobs::BlobId) from its small foldable state. They sit beside,
    /// not in, the journal, on the entity's own shard replicas, and the entity
    /// drives their reclamation from its own live id set
    /// ([`EntityBlobs::gc`](crate::blobs::EntityBlobs::gc)).
    pub fn blobs(&self) -> EntityBlobs {
        // Every sweep through this handle unions the facets' live roots (spec
        // §7.12), so neither the entity nor the host can drop the other's bytes.
        let cell = Arc::clone(&self.facets);
        EntityBlobs::new(Arc::clone(&self.journal), self.name.clone())
            .with_facet_roots(Arc::new(move || cell.roots()))
    }

    /// A shareable self-reference (spec §4.3). It resolves through the gateway on
    /// each call, with no host cache.
    pub fn this(&self) -> EntityRef<G> {
        EntityRef::new(
            self.entity_type,
            self.name.clone(),
            self.gateway.clone(),
            self.system.clone(),
            None,
        )
    }

    pub fn system(&self) -> &G::System {
        &self.system
    }

    /// Up to `limit` of this entity's committed **events** after `from`
    /// (exclusive), decoded, in ascending `Seq` order, the load-path form of
    /// the subscription's event projection (spec §7.9): facet records (§7.12)
    /// are skipped, so the returned `Seq`s are the journal's real slots and may
    /// have gaps between consecutive events. A local, fence-free read of the
    /// leader's journal (§7.3), read-your-leader like any query (§7.5).
    ///
    /// Returns fewer than `limit` events only when the head is reached, so an
    /// empty page means no event follows `from`: the loop pages past facet
    /// records rather than surfacing them as short pages. A record that will
    /// not split or decode is [`EntityJournalError::Unavailable`]: corruption,
    /// never silently skipped.
    ///
    /// The returned [`EventHistory`] carries the store's compaction base (§9):
    /// a `from` below it asked for history the snapshot has subsumed, and the
    /// events in `(from, base]` are not (and can no longer be) served.
    ///
    /// Every page is bounded by the activation's **committed head**, exactly
    /// like the gateway's non-activating read (§7.5): the local store may hold
    /// an in-flight append's tentative slots (§7.2), and this `'static` future
    /// may race the host's own append. Slots above the head are not served
    /// (**E5**).
    pub fn events(&self, from: Seq, limit: usize) -> EventsFuture<G::Event> {
        /// Journal entries per `load` page: sized to a subscription batch, not
        /// to `limit`, so a caller's large `limit` cannot force one huge read.
        const LOAD_PAGE: usize = 256;
        let journal = Arc::clone(&self.journal);
        let committed = Arc::clone(&self.committed);
        let name = self.name.clone();
        let codec = self.system.codec();
        Box::pin(async move {
            let floor = committed.load(std::sync::atomic::Ordering::Acquire);
            if floor == crate::host::HEAD_UNPUBLISHED {
                // Only reachable from a future that escaped a host still
                // rehydrating: the head is being recovered; transient, and a
                // read commits nothing, so re-asking is safe.
                return Err(EntityJournalError::Unavailable(
                    "the entity's committed head is not yet recovered".into(),
                ));
            }
            let floor = Seq::new(floor);
            let mut base = Seq::ZERO;
            let mut events = Vec::new();
            let mut cursor = from;
            while events.len() < limit {
                let page = journal.load(&name, cursor, LOAD_PAGE).await?;
                let (page_base, records, at_head) = committed_prefix(page, floor, LOAD_PAGE);
                // The store's base only advances, so the last page's is the
                // highest this read observed.
                base = page_base;
                if let Some((seq, _)) = records.last() {
                    cursor = *seq;
                }
                for (seq, payload) in events_in_page(records)? {
                    let event = actor_serialization::decode::<G::Event>(&*codec, &payload)
                        .map_err(|e| EntityJournalError::Unavailable(e.to_string()))?;
                    events.push((seq, event));
                    if events.len() == limit {
                        break;
                    }
                }
                if at_head {
                    break;
                }
            }
            Ok(EventHistory { base, events })
        })
    }
}

/// Register `RunTyped<M>` on the host (spec §5.5): a free fn (no captured state)
/// so it can be stored as a plain `fn` pointer in [`EntityRegistry`].
fn register_run_typed<G, M>(registry: &mut HandlerRegistry<Host<G>>)
where
    G: EntityHandler<M>,
    M: Message,
{
    registry.accept::<RunTyped<M>>();
}

/// Maps the commands an entity accepts to its deserialization allowlist (spec
/// §5.5), the entity analogue of `HandlerRegistry`. `Entity::register` fills it via
/// `r.accept::<M>()`. A name-addressed command whose manifest is unregistered
/// surfaces from the transport as `EntityError::Call(CallError::Unhandled)`.
pub struct EntityRegistry<G: Entity> {
    accepted: BTreeSet<&'static str>,
    host_entries: Vec<fn(&mut HandlerRegistry<Host<G>>)>,
    _marker: PhantomData<fn() -> G>,
}

impl<G: Entity> EntityRegistry<G> {
    pub(crate) fn new() -> EntityRegistry<G> {
        EntityRegistry {
            accepted: BTreeSet::new(),
            host_entries: Vec::new(),
            _marker: PhantomData,
        }
    }

    /// Accept command type `M` (spec §5.5).
    pub fn accept<M>(&mut self)
    where
        G: EntityHandler<M>,
        M: Message,
    {
        self.accepted.insert(M::MANIFEST.as_str());
        self.host_entries.push(register_run_typed::<G, M>);
    }

    /// The host-registration thunks, replayed by [`Host::register`] to build the
    /// host's network dispatch table.
    pub(crate) fn host_entries(&self) -> &[fn(&mut HandlerRegistry<Host<G>>)] {
        &self.host_entries
    }
}

/// The set of command manifests an entity type accepts (spec §5.5): the
/// deserialization allowlist, obtained by running [`Entity::register`].
pub fn accepted_manifests<G: Entity>() -> BTreeSet<&'static str> {
    let mut registry = EntityRegistry::<G>::new();
    G::register(&mut registry);
    registry.accepted
}
