//! The per-shard alarm index (spec §16): a durable directory of the pending
//! [`Alarm`](crate::Alarm) deadlines of one grain type's grains, so a node that
//! acquires a shard's leadership can re-activate the grains that owe a callerless
//! wake — **without** scanning every grain or holding them all resident.
//!
//! # Why it exists
//!
//! Phase-1 alarms fire from an in-activation timer and veto idle hibernation, so a
//! resident grain always fires on time. The gap is **failover**: when a shard's
//! leader dies, its grains passivate (a forced step-down, §13) and the new leader
//! activates them only on access — so an alarm with no caller would sleep until the
//! grain is next touched. This index closes that gap. Each [`Host`](crate::Host)
//! registers its grain's pending deadline here on every alarm change; after a leader
//! change, the alarm **driver** reads the index for the shards it now leads and
//! re-activates each pending grain, whose [`on_activate`](crate::Grain::on_activate)
//! re-arms its own timer (**G3**). The index is a *hint* — the grain's own alarm
//! facet is the source of truth — so a stale or missing entry is reconciled on the
//! next activation, never a lost or double fire.
//!
//! # Shape
//!
//! One index grain per **(target grain type, shard)**, keyed
//! [`index_key`](index_key)-style, holding a `GrainName → due-nanos` map. It mirrors
//! [`tenancy::Directory`](../../tenancy/index.html): a small event-sourced map, pure
//! fold, generic over the hosting system so it runs on either durability tier. Reads
//! (`due_before`) serve locally from the activation (§7.5); registration is
//! idempotent (§7.5), so a re-register of an unchanged deadline commits nothing.

use std::collections::BTreeMap;
use std::marker::PhantomData;

use actor_core::Manifest;
use actor_core::Message;
use serde::Deserialize;
use serde::Serialize;

use crate::grain::Grain;
use crate::grain::GrainCtx;
use crate::grain::GrainHandler;
use crate::grain::GrainName;
use crate::grain::GrainRegistry;
use crate::system::GranarySystem;

/// The grain type of the alarm index (spec §16).
pub const ALARM_INDEX_TYPE: &str = "granary.AlarmIndex";

/// The index grain key for one target grain type and shard index. One index grain
/// per (type, shard) keeps each type's driver reading only its own shards, and keeps
/// a single index grain from growing across every type in the cluster.
pub fn index_key(target_type: &str, shard: usize) -> String {
    format!("{target_type}/{shard}")
}

/// One registered grain's state: its pending deadline and the **journal head** of
/// the target grain at the commit that last changed it. The head totally orders a
/// grain's own alarm changes (it advances by every commit and never regresses, even
/// across activations), so the index resolves an out-of-order pair of updates — the
/// register and clear a single activation may emit as independent fire-and-forget
/// tells — by keeping the higher head. A real clear always carries a higher head
/// than the register it supersedes, so it always wins.
#[derive(Clone, Copy, Debug, Serialize, Deserialize)]
struct Slot {
    due: u64,
    head: u64,
}

/// The index's journal record: the unit of durable change (granary §4.1). The fold
/// is pure — no clock, no I/O — so live commit and replay agree (**G2**).
#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum Change {
    /// The grain's alarm is pending for this deadline (ns), as of this journal head.
    Set(GrainName, u64, u64),
    /// The grain has no pending alarm (cleared at this journal head).
    Cleared(GrainName),
}

/// The folded state: the pending deadline of each registered grain.
#[derive(Default, Clone, Debug, Serialize, Deserialize)]
pub struct Pending {
    slots: BTreeMap<GrainName, Slot>,
}

impl Pending {
    /// The registered grains whose deadline is at or before `before` (nanoseconds),
    /// in stable name order — the driver's due set for one sweep.
    pub fn due_before(&self, before: u64) -> Vec<GrainName> {
        self.slots
            .iter()
            .filter(|&(_, slot)| slot.due <= before)
            .map(|(name, _)| name.clone())
            .collect()
    }

    /// Every registered grain and its deadline, in stable order.
    pub fn all(&self) -> Vec<(GrainName, u64)> {
        self.slots.iter().map(|(n, slot)| (n.clone(), slot.due)).collect()
    }

    /// The registered deadline of one grain, if any.
    pub fn get(&self, name: &GrainName) -> Option<u64> {
        self.slots.get(name).map(|slot| slot.due)
    }

    /// The head at which a grain's entry was last written, if registered.
    fn head_of(&self, name: &GrainName) -> Option<u64> {
        self.slots.get(name).map(|slot| slot.head)
    }

    /// How many grains are registered.
    pub fn len(&self) -> usize {
        self.slots.len()
    }

    /// Whether the index is empty.
    pub fn is_empty(&self) -> bool {
        self.slots.is_empty()
    }
}

/// A per-shard alarm index for one target grain type — a durable `GrainName → due`
/// directory (spec §16). Generic over the hosting system so it runs on either
/// durability tier; the activation's state is the folded [`Pending`] map, rebuilt
/// from the journal (granary §1).
pub struct AlarmIndex<S>(PhantomData<fn() -> S>);

// Manual `Default` (not derived): the grain holds only `PhantomData`, so it is
// `Default` for every `S`, whereas the derive would demand `S: Default`.
impl<S> Default for AlarmIndex<S> {
    fn default() -> Self {
        AlarmIndex(PhantomData)
    }
}

impl<S: GranarySystem> Grain for AlarmIndex<S> {
    type System = S;
    type State = Pending;
    type Event = Change;
    type Facets = ();
    const GRAIN_TYPE: &'static str = ALARM_INDEX_TYPE;

    fn apply(state: &mut Pending, event: &Change) {
        match event {
            Change::Set(name, due, head) => {
                state.slots.insert(name.clone(), Slot { due: *due, head: *head });
            }
            Change::Cleared(name) => {
                state.slots.remove(name);
            }
        }
    }

    fn register(r: &mut GrainRegistry<Self>) {
        r.accept::<Sync>();
        r.accept::<DueBefore>();
        r.accept::<AllPending>();
    }
}

/// Sync a grain's alarm state into the index at a journal head (spec §16). `due` is
/// `Some(ns)` while an alarm is pending, `None` once it clears. Applied only if
/// `head` is at least the entry's last head, so an out-of-order pair from one
/// activation resolves to the higher head (see [`Slot`]) — the register and clear a
/// grain emits as independent tells never race to the wrong result. Idempotent: an
/// unchanged `(due, head)` commits nothing (§7.5).
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Sync {
    pub grain: GrainName,
    pub due: Option<u64>,
    pub head: u64,
}
impl Message for Sync {
    type Reply = ();
    const MANIFEST: Manifest = Manifest::new("granary.AlarmIndex.Sync");
}
impl<S: GranarySystem> GrainHandler<Sync> for AlarmIndex<S> {
    async fn handle(&self, state: &Pending, msg: Sync, _ctx: &GrainCtx<Self>) -> (Vec<Change>, ()) {
        let last_head = state.head_of(&msg.grain);
        match msg.due {
            // Register/update: keep the higher head; skip a stale or unchanged update.
            Some(due) => {
                let stale = last_head.is_some_and(|h| msg.head < h);
                let unchanged = state.get(&msg.grain) == Some(due) && last_head == Some(msg.head);
                if stale || unchanged {
                    return (vec![], ());
                }
                (vec![Change::Set(msg.grain, due, msg.head)], ())
            }
            // Clear: remove only when the entry is no newer than this clear, so a
            // stale low-head clear cannot drop a live higher-head register.
            None => match last_head {
                Some(h) if msg.head >= h => (vec![Change::Cleared(msg.grain)], ()),
                _ => (vec![], ()),
            },
        }
    }
}

/// The registered grains due at or before `before` (nanoseconds) — the driver's
/// sweep query. A read; emits no events (§7.5).
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct DueBefore {
    pub before: u64,
}
impl Message for DueBefore {
    type Reply = Vec<GrainName>;
    const MANIFEST: Manifest = Manifest::new("granary.AlarmIndex.DueBefore");
}
impl<S: GranarySystem> GrainHandler<DueBefore> for AlarmIndex<S> {
    async fn handle(&self, state: &Pending, msg: DueBefore, _ctx: &GrainCtx<Self>) -> (Vec<Change>, Vec<GrainName>) {
        (vec![], state.due_before(msg.before))
    }
}

/// Every registered grain and its deadline — for tests and diagnostics. A read.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct AllPending;
impl Message for AllPending {
    type Reply = Vec<(GrainName, u64)>;
    const MANIFEST: Manifest = Manifest::new("granary.AlarmIndex.AllPending");
}
impl<S: GranarySystem> GrainHandler<AllPending> for AlarmIndex<S> {
    async fn handle(&self, state: &Pending, _msg: AllPending, _ctx: &GrainCtx<Self>) -> (Vec<Change>, Vec<(GrainName, u64)>) {
        (vec![], state.all())
    }
}
