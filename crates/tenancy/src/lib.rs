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
//! the set of [`GrainName`]s that principal has created. Each principal's index is
//! its own grain — its own journal, its own blast radius — the direct analogue of
//! "one durable object per user".
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

use std::collections::BTreeSet;
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

/// The folded state of a principal's directory: the set of grain names it owns.
///
/// A `BTreeSet` keeps the fold deterministic (granary invariant G2): replay on any
/// activation reaches byte-identical state, and [`List`] returns a stable order.
#[derive(Default, Clone, Debug, Serialize, Deserialize)]
pub struct Ownership {
    owned: BTreeSet<GrainName>,
}

impl Ownership {
    /// The names this principal owns, in stable order.
    pub fn names(&self) -> impl Iterator<Item = &GrainName> {
        self.owned.iter()
    }

    /// The names this principal owns of one grain type, in stable key order.
    ///
    /// A range scan, not a full scan: names sort by `(grain_type, key)` (the
    /// first field of [`GrainName`]), so one type's names are contiguous — the
    /// scan starts at the type's first name and stops when the type changes.
    pub fn names_of_type<'a>(&'a self, grain_type: &'a str) -> impl Iterator<Item = &'a GrainName> {
        self.owned
            .range(GrainName::new(grain_type, String::new())..)
            .take_while(move |n| n.grain_type() == grain_type)
    }

    /// The distinct grain types this principal owns at least one of, in order.
    ///
    /// The set is sorted by `(grain_type, key)`, so equal types are adjacent and
    /// a single dedup-adjacent pass suffices.
    pub fn types(&self) -> impl Iterator<Item = &str> {
        self.owned
            .iter()
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
        self.owned.len()
    }

    /// Whether this principal owns no grains.
    pub fn is_empty(&self) -> bool {
        self.owned.is_empty()
    }
}

/// The directory's journal record: the unit of durable change (granary §4.1).
///
/// `apply` is the pure fold over these; it never performs I/O or reads the clock,
/// so live commit and replay agree (G2).
#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum Change {
    /// The principal now owns this name.
    Recorded(GrainName),
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
            Change::Recorded(name) => {
                state.owned.insert(name.clone());
            }
            Change::Forgotten(name) => {
                state.owned.remove(name);
            }
            Change::Cleared => state.owned.clear(),
        }
    }

    fn register(r: &mut GrainRegistry<Self>) {
        r.accept::<Record>();
        r.accept::<Forget>();
        r.accept::<Clear>();
        r.accept::<List>();
        r.accept::<ListByType>();
        r.accept::<Types>();
        r.accept::<Contains>();
        r.accept::<Count>();
        r.accept::<CountByType>();
    }
}

/// Record that the principal owns `name`. Idempotent: recording a name already
/// owned commits nothing (granary §7.5) and replies `false`. Replies `true` when
/// the name was newly recorded.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Record {
    pub name: GrainName,
}
impl Message for Record {
    type Reply = bool;
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

/// Enumerate the names the principal owns (the read that ownership exists for).
/// Emits no events; served locally from the activation (granary §7.5).
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct List;
impl Message for List {
    type Reply = Vec<GrainName>;
    const MANIFEST: Manifest = Manifest::new("tenancy.List");
}

/// Enumerate the names the principal owns of one grain type — the query for "all
/// my sessions" when a principal holds several grains per type. A range scan
/// (names sort by `(grain_type, key)`), not a full scan. Emits no events.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ListByType {
    pub grain_type: String,
}
impl Message for ListByType {
    type Reply = Vec<GrainName>;
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

impl<S: GranarySystem> GrainHandler<Record> for Directory<S> {
    async fn handle(&self, state: &Ownership, msg: Record, _: &GrainCtx<Self>) -> (Vec<Change>, bool) {
        if state.owned.contains(&msg.name) {
            (Vec::new(), false)
        } else {
            (vec![Change::Recorded(msg.name)], true)
        }
    }
}

impl<S: GranarySystem> GrainHandler<Forget> for Directory<S> {
    async fn handle(&self, state: &Ownership, msg: Forget, _: &GrainCtx<Self>) -> (Vec<Change>, bool) {
        if state.owned.contains(&msg.name) {
            (vec![Change::Forgotten(msg.name)], true)
        } else {
            (Vec::new(), false)
        }
    }
}

impl<S: GranarySystem> GrainHandler<Clear> for Directory<S> {
    async fn handle(&self, state: &Ownership, _: Clear, _: &GrainCtx<Self>) -> (Vec<Change>, u64) {
        let n = state.owned.len() as u64;
        if n == 0 {
            (Vec::new(), 0)
        } else {
            (vec![Change::Cleared], n)
        }
    }
}

impl<S: GranarySystem> GrainHandler<List> for Directory<S> {
    async fn handle(&self, state: &Ownership, _: List, _: &GrainCtx<Self>) -> (Vec<Change>, Vec<GrainName>) {
        (Vec::new(), state.owned.iter().cloned().collect())
    }
}

impl<S: GranarySystem> GrainHandler<ListByType> for Directory<S> {
    async fn handle(&self, state: &Ownership, msg: ListByType, _: &GrainCtx<Self>) -> (Vec<Change>, Vec<GrainName>) {
        (Vec::new(), state.names_of_type(&msg.grain_type).cloned().collect())
    }
}

impl<S: GranarySystem> GrainHandler<Types> for Directory<S> {
    async fn handle(&self, state: &Ownership, _: Types, _: &GrainCtx<Self>) -> (Vec<Change>, Vec<String>) {
        (Vec::new(), state.types().map(str::to_owned).collect())
    }
}

impl<S: GranarySystem> GrainHandler<CountByType> for Directory<S> {
    async fn handle(&self, state: &Ownership, msg: CountByType, _: &GrainCtx<Self>) -> (Vec<Change>, u64) {
        (Vec::new(), state.names_of_type(&msg.grain_type).count() as u64)
    }
}

impl<S: GranarySystem> GrainHandler<Contains> for Directory<S> {
    async fn handle(&self, state: &Ownership, msg: Contains, _: &GrainCtx<Self>) -> (Vec<Change>, bool) {
        (Vec::new(), state.owned.contains(&msg.name))
    }
}

impl<S: GranarySystem> GrainHandler<Count> for Directory<S> {
    async fn handle(&self, state: &Ownership, _: Count, _: &GrainCtx<Self>) -> (Vec<Change>, u64) {
        (Vec::new(), state.owned.len() as u64)
    }
}
