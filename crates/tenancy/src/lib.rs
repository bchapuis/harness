//! Tenancy: an ownership-index entity layered on entity.
//!
//! The entity layer is deliberately **tenant-blind** (entity spec; `research/durable-objects.md`):
//! its storage core knows only opaque entity names. Multi-tenancy is assembled *above*
//! it: a naming convention (the principal id rides in an entity's key), a capability
//! boundary at each consumer's authenticating edge, and an application-level
//! **directory** that records which entities a principal owns. This crate is that
//! directory. Its seam is the [`Directory`] entity's commands ([`Record`],
//! [`Forget`], [`Clear`], [`Get`], [`List`], [`ListByType`]); the agent
//! runtime's HTTP edge is its consumer, and `docs/edge-spec.md` specifies how
//! the edge records and lists ownership.
//!
//! "Own" means more than "address": it means **enumerate** ("list my entities") and
//! **bulk-forget** ("drop my whole index when I leave"). A [`Directory`] is one
//! entity *per principal*, keyed `(entity_type, principal_id)`, whose journal records
//! the [`EntityName`]s that principal owns, each with the [`Meta`] a listing page
//! renders (a display label, a creation time, free-form attributes).
//!
//! # Metadata is about the *relation*, not the entity
//!
//! [`Meta`] holds facts the directory itself owns: the display label the principal
//! chose, when it recorded the entity, caller-defined attributes. It MUST NOT mirror
//! the target entity's live state (its balance, its status): that would be a second
//! source of truth that drifts the moment the entity changes, reintroducing the
//! cross-entity consistency entity keeps out (each entity is its own boundary, §1.1).
//!
//! Every field is **caller-supplied**, because the fold ([`Entity::apply`]) must be
//! pure and deterministic, no clock, no entropy (entity §4.1). So `created_at` is
//! whatever epoch the caller passes at [`Record`] time; the directory never reads
//! the clock.
//!
//! # Where the boundaries are
//!
//! - **Not in entity.** This crate depends on entity's public API and never the
//!   reverse. The directory is an ordinary entity: it introduces no new
//!   transport, journal, or consensus.
//! - **Not the auth boundary.** *Who* may read or mutate a principal's directory is
//!   decided at each consumer's authenticating edge, where the principal is actually
//!   known, never here. An `EntityRef<Directory>` is an in-cluster capability among
//!   trusted peers, not a tenant-safe token.
//! - **Not a deleter of what it indexes.** The directory *forgets* names; it cannot
//!   reach into another entity. Tearing a principal's entities down is driven by the
//!   consumer: [`List`] the names, then tell each entity to retire, then [`Clear`].
//!
//! # Multiple consumers
//!
//! Hosted under its own `ENTITY_TYPE` (`"tenancy.Directory"`) via
//! [`system.entities::<Directory<S>>(config)`](entity::EntityExt::entities), all
//! consumers share one directory namespace. A consumer that wants an isolated
//! namespace hosts the same entity under its own runtime type name with
//! [`entities_named`](entity::cluster::EntityNode::entities_named) (e.g. `"app.Directory"`):
//! distinct gateways, shard maps, and consensus groups.

use std::collections::BTreeMap;
use std::marker::PhantomData;

use actor_core::Manifest;
use actor_core::Message;
use entity::Entity;
use entity::EntityCtx;
use entity::EntityHandler;
use entity::EntityName;
use entity::EntityRegistry;
use entity::EntitySystem;
use serde::Deserialize;
use serde::Serialize;

/// The listing metadata of one owned entity: the attributes a listing page renders.
///
/// Every field is caller-supplied (the fold reads no clock, entity §4.1) and
/// belongs to the directory, never a copy of the target entity's live state
/// (see the crate docs).
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct Meta {
    /// A human display name for the entry: the listing's title column. Distinct
    /// from the [`EntityName`] key, which is the durable address.
    pub label: Option<String>,
    /// When the principal recorded the entity, as a caller-supplied epoch. The
    /// unit is the caller's; the directory never interprets it.
    pub created_at: Option<u64>,
    /// Free-form attributes for the long tail of listing columns (status, icon,
    /// owner-defined tags). Caller-defined keys and values.
    pub attrs: BTreeMap<String, String>,
}

/// One owned entity and its listing metadata: the unit a [`List`] returns.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Entry {
    pub name: EntityName,
    pub meta: Meta,
}

/// The outcome of a [`Record`]: whether it created the entry, updated its metadata,
/// or found it already present and unchanged (in which case nothing committed).
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum Recorded {
    /// The name was newly recorded.
    Created,
    /// The name was already owned; its metadata changed.
    Updated,
    /// The name was already owned with identical metadata; nothing committed.
    Unchanged,
}

/// The folded state of a principal's directory: the entities it owns and their
/// listing metadata.
///
/// A `BTreeMap` keyed by [`EntityName`] keeps the fold deterministic (entity
/// invariant E2) and, because a name sorts by `(entity_type, key)`, keeps each
/// entity type's entries contiguous, so a [`ListByType`] is a range scan. The
/// state is read only through the directory's own commands.
#[derive(Default, Clone, Debug, Serialize, Deserialize)]
pub struct Ownership {
    entries: BTreeMap<EntityName, Meta>,
}

/// The directory's journal record: the unit of durable change (entity §4.1).
///
/// `apply` is the pure fold over these; it never performs I/O or reads the clock,
/// so live commit and replay agree (E2).
#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum Change {
    /// The principal owns this name with this metadata (insert or metadata update).
    Put(EntityName, Meta),
    /// The principal no longer owns this name.
    Forgotten(EntityName),
    /// The principal's whole index was dropped.
    Cleared,
}

/// A principal's ownership index: one entity per principal, keyed by principal id.
///
/// Generic over the hosting system so it runs on either durability tier (the
/// single-node `Local` tier and the clustered `Quorum` tier, storage §3.2) and on
/// any [`EntitySystem`]. The type carries no state of its own: the activation's
/// state is the folded [`Ownership`], rebuilt from the journal (entity §1).
pub struct Directory<S>(PhantomData<fn() -> S>);

// A manual `Default` (not derived): the directory holds only `PhantomData`, so it
// is `Default` for every `S`, whereas `#[derive(Default)]` would wrongly demand
// `S: Default`. `entity::<Directory<S>>()` builds the behavior via `Default`.
impl<S> Default for Directory<S> {
    fn default() -> Self {
        Directory(PhantomData)
    }
}

impl<S: EntitySystem> Entity for Directory<S> {
    type System = S;
    type State = Ownership;
    type Event = Change;
    type Facets = ();
    const ENTITY_TYPE: &'static str = "tenancy.Directory";

    fn apply(state: &mut Ownership, event: &Change) {
        match event {
            Change::Put(name, meta) => {
                state.entries.insert(name.clone(), meta.clone());
            }
            Change::Forgotten(name) => {
                state.entries.remove(name);
            }
            Change::Cleared => state.entries.clear(),
        }
    }

    fn register(r: &mut EntityRegistry<Self>) {
        r.accept::<Record>();
        r.accept::<Forget>();
        r.accept::<Clear>();
        r.accept::<Get>();
        r.accept::<List>();
        r.accept::<ListByType>();
    }
}

/// Record that the principal owns `name` with `meta`, or update the metadata of a
/// name already owned. Idempotent: recording a name already owned with identical
/// metadata commits nothing and replies [`Recorded::Unchanged`] (entity §7).
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Record {
    pub name: EntityName,
    pub meta: Meta,
}
impl Message for Record {
    type Reply = Recorded;
    const MANIFEST: Manifest = Manifest::new("tenancy.Record");
}

/// Drop `name` from the principal's index. Idempotent: forgetting an absent name
/// commits nothing and replies `false`. Replies `true` when a name was removed.
///
/// This forgets the *index entry* only, it does not delete the entity `name`
/// addresses: physical teardown is the consumer's job (see the crate docs).
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Forget {
    pub name: EntityName,
}
impl Message for Forget {
    type Reply = bool;
    const MANIFEST: Manifest = Manifest::new("tenancy.Forget");
}

/// Drop the principal's entire index in one atomic commit. Replies with the number
/// of names forgotten; commits nothing on an already-empty index.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Clear;
impl Message for Clear {
    type Reply = u64;
    const MANIFEST: Manifest = Manifest::new("tenancy.Clear");
}

/// The metadata for one owned name, or `None` if the principal does not own it. A
/// read; emits no events.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Get {
    pub name: EntityName,
}
impl Message for Get {
    type Reply = Option<Meta>;
    const MANIFEST: Manifest = Manifest::new("tenancy.Get");
}

/// Enumerate the entries the principal owns: name and metadata, enough to render a
/// listing without touching each entity. Emits no events; served locally from the
/// activation (entity §7).
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct List;
impl Message for List {
    type Reply = Vec<Entry>;
    const MANIFEST: Manifest = Manifest::new("tenancy.List");
}

/// Enumerate the entries the principal owns of one entity type: the query for "all
/// my sessions" when a principal holds several entities per type. A range scan over
/// the contiguous type (see [`Ownership`]), not a full scan. Emits no events.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ListByType {
    pub entity_type: String,
}
impl Message for ListByType {
    type Reply = Vec<Entry>;
    const MANIFEST: Manifest = Manifest::new("tenancy.ListByType");
}

fn entry(name: &EntityName, meta: &Meta) -> Entry {
    Entry {
        name: name.clone(),
        meta: meta.clone(),
    }
}

impl<S: EntitySystem> EntityHandler<Record> for Directory<S> {
    async fn handle(
        &self,
        state: &Ownership,
        msg: Record,
        _: &EntityCtx<Self>,
    ) -> (Vec<Change>, Recorded) {
        match state.entries.get(&msg.name) {
            None => (vec![Change::Put(msg.name, msg.meta)], Recorded::Created),
            Some(existing) if *existing != msg.meta => {
                (vec![Change::Put(msg.name, msg.meta)], Recorded::Updated)
            }
            Some(_) => (Vec::new(), Recorded::Unchanged),
        }
    }
}

impl<S: EntitySystem> EntityHandler<Forget> for Directory<S> {
    async fn handle(
        &self,
        state: &Ownership,
        msg: Forget,
        _: &EntityCtx<Self>,
    ) -> (Vec<Change>, bool) {
        if state.entries.contains_key(&msg.name) {
            (vec![Change::Forgotten(msg.name)], true)
        } else {
            (Vec::new(), false)
        }
    }
}

impl<S: EntitySystem> EntityHandler<Clear> for Directory<S> {
    async fn handle(&self, state: &Ownership, _: Clear, _: &EntityCtx<Self>) -> (Vec<Change>, u64) {
        let n = state.entries.len() as u64;
        if n == 0 {
            (Vec::new(), 0)
        } else {
            (vec![Change::Cleared], n)
        }
    }
}

impl<S: EntitySystem> EntityHandler<Get> for Directory<S> {
    async fn handle(
        &self,
        state: &Ownership,
        msg: Get,
        _: &EntityCtx<Self>,
    ) -> (Vec<Change>, Option<Meta>) {
        (Vec::new(), state.entries.get(&msg.name).cloned())
    }
}

impl<S: EntitySystem> EntityHandler<List> for Directory<S> {
    async fn handle(
        &self,
        state: &Ownership,
        _: List,
        _: &EntityCtx<Self>,
    ) -> (Vec<Change>, Vec<Entry>) {
        (
            Vec::new(),
            state.entries.iter().map(|(n, m)| entry(n, m)).collect(),
        )
    }
}

impl<S: EntitySystem> EntityHandler<ListByType> for Directory<S> {
    async fn handle(
        &self,
        state: &Ownership,
        msg: ListByType,
        _: &EntityCtx<Self>,
    ) -> (Vec<Change>, Vec<Entry>) {
        let entries = state
            .entries
            .range(EntityName::new(msg.entity_type.as_str(), String::new())..)
            .take_while(|(n, _)| n.entity_type() == msg.entity_type)
            .map(|(n, m)| entry(n, m))
            .collect();
        (Vec::new(), entries)
    }
}
