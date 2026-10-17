//! The per-shard alarm index (spec facets §4.5): a durable directory of the pending
//! [`Alarm`](crate::alarm::Alarm) deadlines of one entity type's entities, so a node that
//! acquires a shard's leadership can re-activate the entities that owe a callerless
//! wake; **without** scanning every entity or holding them all resident.
//!
//! # Why it exists
//!
//! An alarm fires from an in-activation timer, so a resident entity always fires on
//! time. The gap is every **deactivation**: a failover deactivates a shard's entities
//! (a forced step-down, §10), and idle hibernation would otherwise pin an alarmed
//! entity resident forever, in either case an alarm with no caller would sleep
//! until the entity is next touched. Each [`Host`](crate::host::Host) registers its
//! entity's pending deadline here on every alarm change; the alarm **driver** reads
//! the index for the shards its node leads and re-activates each pending entity,
//! whose host re-arms the timer from the folded deadline as rehydration ends (**E3**).
//! The index is a *hint* (the entity's own alarm facet is the source of truth), so a
//! stale entry is reconciled on the next activation, never a lost or double fire. The
//! one place a *missing* entry would be unrecoverable is a voluntarily hibernated
//! entity, which is why the host hibernates an alarmed entity only after its
//! registration is **acked** (host.rs).
//!
//! # Shape
//!
//! One index entity per **(target entity type, shard)**, keyed
//! [`index_key`]-style, holding an `EntityName → due-nanos` map: a small
//! event-sourced map with a pure fold, mirroring
//! [`tenancy::Directory`](../../tenancy/index.html). Reads
//! (`due_before`) serve locally from the activation (§6); registration is
//! idempotent (§6), so a re-register of an unchanged deadline commits nothing.
//! Registration is an **acknowledged** `ask`: the reply releases only after this
//! entity's commit (the output gate, §5), which is what lets a host treat an acked
//! deadline as durably indexed and hibernate with its alarm pending (host.rs).

use std::collections::BTreeMap;
use std::marker::PhantomData;

use actor_core::Manifest;
use actor_core::Message;
use serde::Deserialize;
use serde::Serialize;

use crate::entity::Entity;
use crate::entity::EntityCtx;
use crate::entity::EntityHandler;
use crate::entity::EntityName;
use crate::entity::EntityRegistry;
use crate::system::EntitySystem;

pub const ALARM_INDEX_TYPE: &str = "entity.AlarmIndex";

/// The index entity key for one target entity type and shard index.
pub fn index_key(target_type: &str, shard: usize) -> String {
    format!("{target_type}/{shard}")
}

/// One registered entity's state: its pending deadline and the **journal head** of
/// the target entity at the commit that last changed it. The head totally orders an
/// entity's own alarm changes (it advances by every commit and never regresses, even
/// across activations), so the index resolves an out-of-order pair of updates (the
/// register and clear a single activation may emit as independently launched
/// asks) by keeping the higher head.
#[derive(Clone, Copy, Debug, Serialize, Deserialize)]
struct Slot {
    due: u64,
    head: u64,
}

/// The index's journal record: the unit of durable change (entity §3.1). The fold
/// is pure (no clock, no I/O), so live commit and replay agree (**E2**).
#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum Change {
    /// The entity's alarm is pending for this deadline (ns), as of this journal head.
    Set(EntityName, u64, u64),
    /// The entity has no pending alarm (cleared at this journal head).
    Cleared(EntityName),
}

/// The folded state: the pending deadline of each registered entity.
#[derive(Default, Clone, Debug, Serialize, Deserialize)]
pub struct Pending {
    slots: BTreeMap<EntityName, Slot>,
}

impl Pending {
    /// The registered entities whose deadline is at or before `before` (nanoseconds),
    /// in stable name order: the driver's due set for one sweep.
    pub fn due_before(&self, before: u64) -> Vec<EntityName> {
        self.slots
            .iter()
            .filter(|&(_, slot)| slot.due <= before)
            .map(|(name, _)| name.clone())
            .collect()
    }

    /// Every registered entity and its deadline, in stable order.
    pub fn all(&self) -> Vec<(EntityName, u64)> {
        self.slots
            .iter()
            .map(|(n, slot)| (n.clone(), slot.due))
            .collect()
    }

    /// The registered deadline of one entity, if any.
    pub fn get(&self, name: &EntityName) -> Option<u64> {
        self.slots.get(name).map(|slot| slot.due)
    }

    fn head_of(&self, name: &EntityName) -> Option<u64> {
        self.slots.get(name).map(|slot| slot.head)
    }
}

/// A per-shard alarm index for one target entity type: a durable `EntityName → due`
/// directory (spec facets §4.5). Generic over the hosting system so it runs on either
/// durability tier; the activation's state is the folded [`Pending`] map, rebuilt
/// from the journal (entity §1).
pub struct AlarmIndex<S>(PhantomData<fn() -> S>);

// Manual `Default` (not derived): the entity holds only `PhantomData`, so it is
// `Default` for every `S`, whereas the derive would demand `S: Default`.
impl<S> Default for AlarmIndex<S> {
    fn default() -> Self {
        AlarmIndex(PhantomData)
    }
}

impl<S: EntitySystem> Entity for AlarmIndex<S> {
    type System = S;
    type State = Pending;
    type Event = Change;
    type Facets = ();
    const ENTITY_TYPE: &'static str = ALARM_INDEX_TYPE;

    fn apply(state: &mut Pending, event: &Change) {
        match event {
            Change::Set(name, due, head) => {
                state.slots.insert(
                    name.clone(),
                    Slot {
                        due: *due,
                        head: *head,
                    },
                );
            }
            Change::Cleared(name) => {
                state.slots.remove(name);
            }
        }
    }

    fn register(r: &mut EntityRegistry<Self>) {
        r.accept::<Sync>();
        r.accept::<DueBefore>();
        r.accept::<AllPending>();
    }
}

/// Sync an entity's alarm state into the index at a journal head (spec facets §4.5). `due` is
/// `Some(ns)` while an alarm is pending, `None` once it clears. Applied only if
/// `head` is at least the entry's last head, so an out-of-order pair from one
/// activation resolves to the higher head (see `Slot`). Idempotent: an
/// unchanged `(due, head)` commits nothing (§6).
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Sync {
    pub entity: EntityName,
    pub due: Option<u64>,
    pub head: u64,
}
impl Message for Sync {
    type Reply = ();
    const MANIFEST: Manifest = Manifest::new("entity.AlarmIndex.Sync");
}
impl<S: EntitySystem> EntityHandler<Sync> for AlarmIndex<S> {
    async fn handle(
        &self,
        state: &Pending,
        msg: Sync,
        _ctx: &EntityCtx<Self>,
    ) -> (Vec<Change>, ()) {
        let last_head = state.head_of(&msg.entity);
        match msg.due {
            Some(due) => {
                let stale = last_head.is_some_and(|h| msg.head < h);
                let unchanged = state.get(&msg.entity) == Some(due) && last_head == Some(msg.head);
                if stale || unchanged {
                    return (vec![], ());
                }
                (vec![Change::Set(msg.entity, due, msg.head)], ())
            }
            // Clear: remove only when the entry is no newer than this clear, so a
            // stale low-head clear cannot drop a live higher-head register.
            None => match last_head {
                Some(h) if msg.head >= h => (vec![Change::Cleared(msg.entity)], ()),
                _ => (vec![], ()),
            },
        }
    }
}

/// The registered entities due at or before `before` (nanoseconds): the driver's
/// sweep query. A read; emits no events (§6).
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct DueBefore {
    pub before: u64,
}
impl Message for DueBefore {
    type Reply = Vec<EntityName>;
    const MANIFEST: Manifest = Manifest::new("entity.AlarmIndex.DueBefore");
}
impl<S: EntitySystem> EntityHandler<DueBefore> for AlarmIndex<S> {
    async fn handle(
        &self,
        state: &Pending,
        msg: DueBefore,
        _ctx: &EntityCtx<Self>,
    ) -> (Vec<Change>, Vec<EntityName>) {
        (vec![], state.due_before(msg.before))
    }
}

/// Every registered entity and its deadline, for tests and diagnostics. A read.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct AllPending;
impl Message for AllPending {
    type Reply = Vec<(EntityName, u64)>;
    const MANIFEST: Manifest = Manifest::new("entity.AlarmIndex.AllPending");
}
impl<S: EntitySystem> EntityHandler<AllPending> for AlarmIndex<S> {
    async fn handle(
        &self,
        state: &Pending,
        _msg: AllPending,
        _ctx: &EntityCtx<Self>,
    ) -> (Vec<Change>, Vec<(EntityName, u64)>) {
        (vec![], state.all())
    }
}
