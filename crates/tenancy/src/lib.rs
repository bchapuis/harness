//! Tenancy: an ownership-index grain layered on granary.
//!
//! Granary is deliberately **tenant-blind** (granary spec; `research/durable-objects.md`):
//! its storage core knows only opaque grain names, exactly as Cloudflare's storage
//! relay knows only object ids. Multi-tenancy is assembled *above* it — a naming
//! convention (the principal id rides in a grain's key), a capability boundary at
//! each consumer's authenticating edge, and an application-level **directory** that
//! records which grains a principal owns. This crate is that directory, and nothing
//! else.
//!
//! "Own" means more than "address": it means **enumerate** ("list my grains") and
//! **bulk-forget** ("drop my whole index when I leave") — the parts a durable-object
//! platform does not hand you for free (DO research §6: ownership is an
//! application-maintained index, not a runtime feature). A [`Directory`] is one
//! grain *per principal*, keyed `(grain_type, principal_id)`, whose journal records
//! the [`GrainName`]s that principal owns, each with the [`Meta`] a listing page
//! renders (a display label, a creation time, free-form attributes).
//!
//! # Metadata is about the *relation*, not the grain
//!
//! [`Meta`] holds facts the directory itself owns — the display label the principal
//! chose, when it recorded the grain, caller-defined attributes. It MUST NOT mirror
//! the target grain's live state (its balance, its status): that would be a second
//! source of truth that drifts the moment the grain changes, reintroducing the
//! cross-grain consistency granary keeps out (each grain is its own boundary, §2.2).
//! A consumer that wants a denormalized view maintains a separate read-model with
//! idempotent updates; it is not the directory's job.
//!
//! Every field is **caller-supplied**, because the fold ([`Grain::apply`]) must be
//! pure and deterministic — no clock, no entropy (granary §4.1). So `created_at` is
//! whatever epoch the caller passes at [`Record`] time; the directory never reads
//! the clock.
//!
//! # Where the boundaries are
//!
//! - **Not in granary.** This crate depends on granary's public API and never the
//!   reverse; the storage core stays tenant-blind. The directory is an ordinary
//!   grain — it introduces no new transport, journal, or consensus.
//! - **Not the auth boundary.** *Who* may read or mutate a principal's directory is
//!   decided at each consumer's authenticating edge (the Worker/binding analogue),
//!   where the principal is actually known — never here. A `GrainRef<Directory>` is
//!   an in-cluster capability among trusted peers, not a tenant-safe token.
//! - **Not a deleter of what it indexes.** The directory *forgets* names; it cannot
//!   reach into another grain. Tearing a principal's grains down is driven by the
//!   consumer: [`List`] the names, then tell each grain to retire, then [`Clear`].
//!
//! # Multiple consumers
//!
//! Hosted under its own `GRAIN_TYPE` (`"tenancy.Directory"`) via
//! [`system.granary::<Directory<S>>(config)`](granary::GranaryExt::granary), all
//! consumers share one directory namespace. A consumer that wants an isolated
//! namespace hosts the same grain under its own runtime type name with
//! [`granary_named`](granary::GranaryExt::granary_named) (e.g. `"app.Directory"`) —
//! distinct gateways, shard maps, and consensus groups, no code change here.

use std::collections::BTreeMap;
use std::marker::PhantomData;

use actor_core::Manifest;
use actor_core::Message;
use granary::Grain;
use granary::GrainCtx;
use granary::GrainHandler;
use granary::GrainName;
use granary::GrainRegistry;
use granary::GranarySystem;
use serde::Deserialize;
use serde::Serialize;

/// The listing metadata of one owned grain — the attributes a listing page renders.
///
/// Every field describes the *ownership entry*, is caller-supplied (the fold reads
/// no clock, granary §4.1), and is the directory's own data — never a copy of the
/// target grain's live state (see the crate docs).
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct Meta {
    /// A human display name for the entry — the listing's title column. Distinct
    /// from the [`GrainName`] key, which is the durable address.
    pub label: Option<String>,
    /// When the principal recorded the grain, as a caller-supplied epoch (the unit
    /// is the caller's; the directory never interprets or compares it to a clock).
    pub created_at: Option<u64>,
    /// Free-form attributes for the long tail of listing columns (status, icon,
    /// owner-defined tags). Caller-defined keys and values.
    pub attrs: BTreeMap<String, String>,
}

/// One owned grain and its listing metadata — the unit a [`List`] returns.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Entry {
    pub name: GrainName,
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

/// The folded state of a principal's directory: the grains it owns and their
/// listing metadata.
///
/// A `BTreeMap` keyed by [`GrainName`] keeps the fold deterministic (granary
/// invariant G2) and, because a name sorts by `(grain_type, key)`, keeps each
/// grain type's entries contiguous — so by-type queries are range scans (§
/// [`Ownership::entries_of_type`]).
#[derive(Default, Clone, Debug, Serialize, Deserialize)]
pub struct Ownership {
    entries: BTreeMap<GrainName, Meta>,
}

impl Ownership {
    /// The names this principal owns, in stable order.
    pub fn names(&self) -> impl Iterator<Item = &GrainName> {
        self.entries.keys()
    }

    /// The entries this principal owns — name and metadata — in stable order.
    pub fn entries(&self) -> impl Iterator<Item = (&GrainName, &Meta)> {
        self.entries.iter()
    }

    /// The metadata for one owned name, if owned.
    pub fn get(&self, name: &GrainName) -> Option<&Meta> {
        self.entries.get(name)
    }

    /// The names this principal owns of one grain type, in stable key order.
    ///
    /// A range scan, not a full scan: names sort by `(grain_type, key)` (the first
    /// field of [`GrainName`]), so one type's names are contiguous — the scan starts
    /// at the type's first name and stops when the type changes.
    pub fn names_of_type<'a>(&'a self, grain_type: &'a str) -> impl Iterator<Item = &'a GrainName> {
        self.entries_of_type(grain_type).map(|(n, _)| n)
    }

    /// The entries this principal owns of one grain type — name and metadata — in
    /// stable key order. The same range scan as [`names_of_type`](Self::names_of_type).
    pub fn entries_of_type<'a>(&'a self, grain_type: &'a str) -> impl Iterator<Item = (&'a GrainName, &'a Meta)> {
        self.entries
            .range(GrainName::new(grain_type, String::new())..)
            .take_while(move |(n, _)| n.grain_type() == grain_type)
    }

    /// The distinct grain types this principal owns at least one of, in order.
    ///
    /// The map is sorted by `(grain_type, key)`, so equal types are adjacent and a
    /// single dedup-adjacent pass suffices.
    pub fn types(&self) -> impl Iterator<Item = &str> {
        self.entries
            .keys()
            .map(GrainName::grain_type)
            .scan(None, |last, t| {
                let fresh = *last != Some(t);
                *last = Some(t);
                Some((fresh, t))
            })
            .filter(|(fresh, _)| *fresh)
            .map(|(_, t)| t)
    }

    /// How many grains this principal owns.
    pub fn len(&self) -> usize {
        self.entries.len()
    }

    /// Whether this principal owns no grains.
    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }
}

/// The directory's journal record: the unit of durable change (granary §4.1).
///
/// `apply` is the pure fold over these; it never performs I/O or reads the clock,
/// so live commit and replay agree (G2).
#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum Change {
    /// The principal owns this name with this metadata (insert or metadata update).
    Put(GrainName, Meta),
    /// The principal no longer owns this name.
    Forgotten(GrainName),
    /// The principal's whole index was dropped.
    Cleared,
}

/// A principal's ownership index — one grain per principal, keyed by principal id.
///
/// Generic over the hosting system so it runs on either durability tier (the
/// single-node `Local` tier and the clustered `Quorum` tier, granary §7.4) and on
/// any [`GranarySystem`]. The type carries no state of its own: the activation's
/// state is the folded [`Ownership`], rebuilt from the journal (granary §1).
pub struct Directory<S>(PhantomData<fn() -> S>);

// A manual `Default` (not derived): the directory holds only `PhantomData`, so it
// is `Default` for every `S`, whereas `#[derive(Default)]` would wrongly demand
// `S: Default`. `granary::<Directory<S>>()` builds the behavior via `Default`.
impl<S> Default for Directory<S> {
    fn default() -> Self {
        Directory(PhantomData)
    }
}

impl<S: GranarySystem> Grain for Directory<S> {
    type System = S;
    type State = Ownership;
    type Event = Change;
    const GRAIN_TYPE: &'static str = "tenancy.Directory";

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

    fn register(r: &mut GrainRegistry<Self>) {
        r.accept::<Record>();
        r.accept::<Forget>();
        r.accept::<Clear>();
        r.accept::<Get>();
        r.accept::<List>();
        r.accept::<ListByType>();
        r.accept::<Types>();
        r.accept::<Contains>();
        r.accept::<Count>();
        r.accept::<CountByType>();
    }
}

/// Record that the principal owns `name` with `meta`, or update the metadata of a
/// name already owned. Idempotent: recording a name already owned with identical
/// metadata commits nothing and replies [`Recorded::Unchanged`] (granary §7.5).
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Record {
    pub name: GrainName,
    pub meta: Meta,
}
impl Message for Record {
    type Reply = Recorded;
    const MANIFEST: Manifest = Manifest::new("tenancy.Record");
}

/// Drop `name` from the principal's index. Idempotent: forgetting an absent name
/// commits nothing and replies `false`. Replies `true` when a name was removed.
///
/// This forgets the *index entry* only — it does not delete the grain `name`
/// addresses (see the crate docs: physical teardown is the consumer's job).
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Forget {
    pub name: GrainName,
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
    pub name: GrainName,
}
impl Message for Get {
    type Reply = Option<Meta>;
    const MANIFEST: Manifest = Manifest::new("tenancy.Get");
}

/// Enumerate the entries the principal owns — name and metadata (the read that
/// ownership exists for, enough to render a listing without touching each grain).
/// Emits no events; served locally from the activation (granary §7.5).
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct List;
impl Message for List {
    type Reply = Vec<Entry>;
    const MANIFEST: Manifest = Manifest::new("tenancy.List");
}

/// Enumerate the entries the principal owns of one grain type — the query for "all
/// my sessions" when a principal holds several grains per type. A range scan (names
/// sort by `(grain_type, key)`), not a full scan. Emits no events.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ListByType {
    pub grain_type: String,
}
impl Message for ListByType {
    type Reply = Vec<Entry>;
    const MANIFEST: Manifest = Manifest::new("tenancy.ListByType");
}

/// The distinct grain types the principal owns at least one of. A read; emits no
/// events.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Types;
impl Message for Types {
    type Reply = Vec<String>;
    const MANIFEST: Manifest = Manifest::new("tenancy.Types");
}

/// Whether the principal owns `name`. A read; emits no events.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Contains {
    pub name: GrainName,
}
impl Message for Contains {
    type Reply = bool;
    const MANIFEST: Manifest = Manifest::new("tenancy.Contains");
}

/// How many grains the principal owns. A read; emits no events.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Count;
impl Message for Count {
    type Reply = u64;
    const MANIFEST: Manifest = Manifest::new("tenancy.Count");
}

/// How many grains of one grain type the principal owns. A range scan; emits no
/// events.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct CountByType {
    pub grain_type: String,
}
impl Message for CountByType {
    type Reply = u64;
    const MANIFEST: Manifest = Manifest::new("tenancy.CountByType");
}

fn entry(name: &GrainName, meta: &Meta) -> Entry {
    Entry {
        name: name.clone(),
        meta: meta.clone(),
    }
}

impl<S: GranarySystem> GrainHandler<Record> for Directory<S> {
    async fn handle(&self, state: &Ownership, msg: Record, _: &GrainCtx<Self>) -> (Vec<Change>, Recorded) {
        match state.entries.get(&msg.name) {
            None => (vec![Change::Put(msg.name, msg.meta)], Recorded::Created),
            Some(existing) if *existing != msg.meta => (vec![Change::Put(msg.name, msg.meta)], Recorded::Updated),
            Some(_) => (Vec::new(), Recorded::Unchanged),
        }
    }
}

impl<S: GranarySystem> GrainHandler<Forget> for Directory<S> {
    async fn handle(&self, state: &Ownership, msg: Forget, _: &GrainCtx<Self>) -> (Vec<Change>, bool) {
        if state.entries.contains_key(&msg.name) {
            (vec![Change::Forgotten(msg.name)], true)
        } else {
            (Vec::new(), false)
        }
    }
}

impl<S: GranarySystem> GrainHandler<Clear> for Directory<S> {
    async fn handle(&self, state: &Ownership, _: Clear, _: &GrainCtx<Self>) -> (Vec<Change>, u64) {
        let n = state.entries.len() as u64;
        if n == 0 {
            (Vec::new(), 0)
        } else {
            (vec![Change::Cleared], n)
        }
    }
}

impl<S: GranarySystem> GrainHandler<Get> for Directory<S> {
    async fn handle(&self, state: &Ownership, msg: Get, _: &GrainCtx<Self>) -> (Vec<Change>, Option<Meta>) {
        (Vec::new(), state.entries.get(&msg.name).cloned())
    }
}

impl<S: GranarySystem> GrainHandler<List> for Directory<S> {
    async fn handle(&self, state: &Ownership, _: List, _: &GrainCtx<Self>) -> (Vec<Change>, Vec<Entry>) {
        (Vec::new(), state.entries.iter().map(|(n, m)| entry(n, m)).collect())
    }
}

impl<S: GranarySystem> GrainHandler<ListByType> for Directory<S> {
    async fn handle(&self, state: &Ownership, msg: ListByType, _: &GrainCtx<Self>) -> (Vec<Change>, Vec<Entry>) {
        (Vec::new(), state.entries_of_type(&msg.grain_type).map(|(n, m)| entry(n, m)).collect())
    }
}

impl<S: GranarySystem> GrainHandler<Types> for Directory<S> {
    async fn handle(&self, state: &Ownership, _: Types, _: &GrainCtx<Self>) -> (Vec<Change>, Vec<String>) {
        (Vec::new(), state.types().map(str::to_owned).collect())
    }
}

impl<S: GranarySystem> GrainHandler<Contains> for Directory<S> {
    async fn handle(&self, state: &Ownership, msg: Contains, _: &GrainCtx<Self>) -> (Vec<Change>, bool) {
        (Vec::new(), state.entries.contains_key(&msg.name))
    }
}

impl<S: GranarySystem> GrainHandler<Count> for Directory<S> {
    async fn handle(&self, state: &Ownership, _: Count, _: &GrainCtx<Self>) -> (Vec<Change>, u64) {
        (Vec::new(), state.entries.len() as u64)
    }
}

impl<S: GranarySystem> GrainHandler<CountByType> for Directory<S> {
    async fn handle(&self, state: &Ownership, msg: CountByType, _: &GrainCtx<Self>) -> (Vec<Change>, u64) {
        (Vec::new(), state.names_of_type(&msg.grain_type).count() as u64)
    }
}
