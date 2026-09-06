//! Shared simulation invariants over entity events (entity spec §7, spec §18.5).
//!
//! These are safety predicates every entity-backed swarm wants: commits advance,
//! and an entity is live at most once per node. They are claims about *entity's*
//! contract, not about any one suite, so they live here rather than in each test
//! binary.
//!
//! Each is constructed with the label it reports under, so a suite still names
//! its own violations: `machine-commit-monotonic` and `disk-entity-commit-monotonic`
//! are the same predicate observed from different workloads.
//!
//! Behind the `testing` feature: test support, which should not ship in a
//! production build of the crate.

use std::collections::BTreeMap;
use std::collections::BTreeSet;

use actor_core::Event;
use actor_core::NodeId;
use actor_simulation::Invariant;

use crate::EntityEvent;
use crate::EntityName;

/// **Commit monotonicity** (invariants **E3**/**E5**): an entity's committed
/// sequence strictly increases.
///
/// A commit at a sequence at or below the current head means two writers both
/// believed themselves authoritative — a minority "leader" that committed, or a
/// replayed entry accepted twice. Either is a split of the commit log.
pub struct CommitMonotonic {
    label: &'static str,
    noun: &'static str,
    last: BTreeMap<EntityName, u64>,
}

impl CommitMonotonic {
    /// Observe under `label`, describing the subject as `noun` in violations —
    /// "entity" for a plain entity suite, "machine" where the entity is a machine.
    pub fn new(label: &'static str, noun: &'static str) -> CommitMonotonic {
        CommitMonotonic {
            label,
            noun,
            last: BTreeMap::new(),
        }
    }
}

impl Invariant for CommitMonotonic {
    fn name(&self) -> &'static str {
        self.label
    }

    fn observe(&mut self, event: &Event) -> Result<(), String> {
        if let Some(EntityEvent::Committed { name, seq, .. }) = event.as_app::<EntityEvent>() {
            let prev = self.last.get(name).copied().unwrap_or(0);
            if *seq <= prev {
                return Err(format!(
                    "{} {name} committed seq {seq} not after previous head {prev} (E3/E5)",
                    self.noun,
                ));
            }
            self.last.insert(name.clone(), *seq);
        }
        Ok(())
    }
}

/// **Exactly-once activation per node** (invariant **E6**): on any one node, a
/// entity is never live twice at once.
///
/// Keyed by `(node, name)`, so an activation that migrates to another leader on
/// failover is not mistaken for a second one. Crash-sound: a node's live set is
/// cleared when the stream reports that node `NodeDown` — its activations are
/// gone with it — so a re-activation after the node rejoins and re-leads is not
/// a false positive.
///
/// **Not** [`actor_simulation::SingletonAtMostOnePerNode`], despite the similar
/// name: that one is the *cluster-utilities* singleton (U2), keyed off
/// `Event::SingletonStarted`. This is the entity analogue, keyed off
/// `EntityEvent::Activated`.
pub struct ActivationSingletonPerNode {
    label: &'static str,
    noun: &'static str,
    live: BTreeSet<(NodeId, EntityName)>,
}

impl ActivationSingletonPerNode {
    /// Observe under `label`, describing the subject as `noun` in violations.
    pub fn new(label: &'static str, noun: &'static str) -> ActivationSingletonPerNode {
        ActivationSingletonPerNode {
            label,
            noun,
            live: BTreeSet::new(),
        }
    }
}

impl Invariant for ActivationSingletonPerNode {
    fn name(&self) -> &'static str {
        self.label
    }

    fn observe(&mut self, event: &Event) -> Result<(), String> {
        // A node declared down loses its activations; drop them so a later
        // re-activation on the recovered node is sound (E6 is per live node).
        if let Event::NodeDown { node, .. } = event {
            self.live.retain(|(n, _)| n != node);
            return Ok(());
        }
        match event.as_app::<EntityEvent>() {
            Some(EntityEvent::Activated { node, name }) => {
                let fresh = self.live.insert((*node, name.clone()));
                if !fresh {
                    return Err(format!(
                        "{} {name} activated while already live on {node} (E6)",
                        self.noun,
                    ));
                }
            }
            Some(EntityEvent::Deactivated { node, name }) => {
                self.live.remove(&(*node, name.clone()));
            }
            _ => {}
        }
        Ok(())
    }

    fn forget_node(&mut self, node: NodeId) {
        // The other way a node's activations vanish: its process ended. Nothing
        // emits `Deactivated` for an activation that died with its host, and the
        // successor may legitimately re-activate it (E6 is per *live* node).
        self.live.retain(|(n, _)| *n != node);
    }
}

/// An [`EntityStore`](crate::EntityStore) with a fixed answer: every write returns
/// `refusal`, every read is empty, every enumeration is empty.
///
/// The deterministic stand-in for a store that never acknowledges — an index whose
/// registrations always fail, a replica permanently fenced — where a real store
/// would need a fault injector to reach the same state. `StaticEntityStore::fenced()`
/// is the common case: writes refused at an unreachably high term.
pub struct StaticEntityStore {
    refusal: crate::StoreAck,
}

impl StaticEntityStore {
    /// A store whose every write is refused with `refusal`.
    pub fn new(refusal: crate::StoreAck) -> StaticEntityStore {
        StaticEntityStore { refusal }
    }

    /// A store fenced at an unreachably high term, so nothing hosted on it can
    /// commit while reads still succeed — activation works and the failure surfaces
    /// where it does in production, at the commit.
    pub fn fenced() -> StaticEntityStore {
        StaticEntityStore::new(crate::StoreAck::Fenced(crate::Term::new(u64::MAX)))
    }

    /// A factory handing this store to every node, for
    /// [`EntityConfig::entity_store`](crate::EntityConfig).
    pub fn factory(refusal: crate::StoreAck) -> crate::EntityStoreFactory {
        std::sync::Arc::new(move |_, _| {
            std::sync::Arc::new(StaticEntityStore::new(refusal.clone()))
                as std::sync::Arc<dyn crate::EntityStore>
        })
    }
}

impl crate::EntityBlobStore for StaticEntityStore {
    fn put_blob(
        &self,
        _shard: u32,
        _entity: &EntityName,
        _id: crate::BlobId,
        _bytes: Vec<u8>,
    ) -> crate::BlobAck {
        crate::BlobAck::Stored
    }

    fn get_blob(&self, _shard: u32, _entity: &EntityName, _id: crate::BlobId) -> Option<Vec<u8>> {
        None
    }

    fn has_blob(&self, _shard: u32, _entity: &EntityName, _id: crate::BlobId) -> bool {
        false
    }

    fn delete_blob(&self, _shard: u32, _entity: &EntityName, _id: crate::BlobId) {}

    fn delete_blobs(&self, _shard: u32, _entity: &EntityName) {}

    fn retain_blobs(&self, _shard: u32, _entity: &EntityName, _retain: &BTreeSet<crate::BlobId>) {}

    fn blob_ids(&self, _shard: u32, _entity: &EntityName) -> Vec<crate::BlobId> {
        Vec::new()
    }
}

impl crate::EntityStore for StaticEntityStore {
    fn store_record(
        &self,
        _shard: u32,
        _entity: &EntityName,
        _after: crate::Seq,
        _term: crate::Term,
        _records: Vec<Vec<u8>>,
        _kind: crate::WriteKind,
    ) -> crate::StoreAck {
        self.refusal.clone()
    }

    fn read(&self, _shard: u32, _entity: &EntityName) -> crate::ReadReply {
        crate::ReadReply::default()
    }

    fn read_from(
        &self,
        _shard: u32,
        _entity: &EntityName,
        _from: crate::Seq,
        _limit: usize,
    ) -> crate::RecordPage {
        crate::RecordPage::default()
    }

    fn prepare(&self, _shard: u32, _entity: &EntityName, _term: crate::Term) -> crate::ReadOutcome {
        // Prepared, not fenced: a read must succeed so activation gets far enough
        // for the refusal to land at the commit.
        crate::ReadOutcome::Prepared(crate::ReadReply::default())
    }

    fn store_snapshot(
        &self,
        _shard: u32,
        _entity: &EntityName,
        _at: crate::Seq,
        _term: crate::Term,
        _state: Vec<u8>,
        _kind: crate::WriteKind,
    ) -> crate::StoreAck {
        self.refusal.clone()
    }

    fn truncate(&self, _shard: u32, _entity: &EntityName, _after: crate::Seq, _term: crate::Term) {}

    fn entities(&self, _shard: u32) -> Vec<EntityName> {
        Vec::new()
    }

    fn seal_range(&self, _shard: u32, _from: u64) {}

    fn unseal(&self, _shard: u32) {}

    fn remove_entity(&self, _shard: u32, _entity: &EntityName) {}

    fn remove_range(&self, _shard: u32, _from: u64) {}

    fn drop_shard(&self, _shard: u32) {}

    fn shard_bytes(&self, _shard: u32) -> u64 {
        0
    }
}

/// An [`EntityStore`](crate::EntityStore) wrapper that reports its snapshot at a seq
/// `ahead` slots **beyond** the one it was saved at, leaving records, head, and
/// snapshot bytes untouched.
///
/// The deterministic stand-in for the state **E4** exists to survive: an entity whose
/// latest durable snapshot names a seq past its committed head. In production that is
/// the `Quorum` tier's doing — `head` is recovered from a write quorum (§8, **E14**)
/// while the snapshot is read from whichever replica holds the newest one, so a
/// snapshot saved by a former leader can outrun the head a quorum will admit. A
/// single-node store can never disagree with itself that way, and reaching it through
/// a cluster would take a fault injector timed between the two reads. Overstating the
/// seq reproduces exactly the disagreement the rule decides, with nothing else moved.
///
/// The host must ignore such a snapshot and replay the whole log from `Seq::ZERO`: the
/// journal is the authority (**E3**), and seeding from a snapshot the head does not
/// cover would fold a state no committed prefix supports.
pub struct SnapshotAheadOfHeadStore {
    inner: std::sync::Arc<dyn crate::EntityStore>,
    ahead: u64,
}

impl SnapshotAheadOfHeadStore {
    /// Wrap `inner`, reporting every snapshot `ahead` slots past its real seq.
    pub fn new(
        inner: std::sync::Arc<dyn crate::EntityStore>,
        ahead: u64,
    ) -> SnapshotAheadOfHeadStore {
        SnapshotAheadOfHeadStore { inner, ahead }
    }

    /// A factory giving every store its own wrapped [`MemoryEntityStore`](crate::MemoryEntityStore),
    /// for [`EntityConfig::entity_store`](crate::EntityConfig).
    pub fn factory(ahead: u64) -> crate::EntityStoreFactory {
        std::sync::Arc::new(move |_, _| {
            std::sync::Arc::new(SnapshotAheadOfHeadStore::new(
                std::sync::Arc::new(crate::MemoryEntityStore::new()),
                ahead,
            )) as std::sync::Arc<dyn crate::EntityStore>
        })
    }
}

impl crate::EntityBlobStore for SnapshotAheadOfHeadStore {
    fn put_blob(
        &self,
        shard: u32,
        entity: &EntityName,
        id: crate::BlobId,
        bytes: Vec<u8>,
    ) -> crate::BlobAck {
        self.inner.put_blob(shard, entity, id, bytes)
    }

    fn get_blob(&self, shard: u32, entity: &EntityName, id: crate::BlobId) -> Option<Vec<u8>> {
        self.inner.get_blob(shard, entity, id)
    }

    fn has_blob(&self, shard: u32, entity: &EntityName, id: crate::BlobId) -> bool {
        self.inner.has_blob(shard, entity, id)
    }

    fn delete_blob(&self, shard: u32, entity: &EntityName, id: crate::BlobId) {
        self.inner.delete_blob(shard, entity, id)
    }

    fn delete_blobs(&self, shard: u32, entity: &EntityName) {
        self.inner.delete_blobs(shard, entity)
    }

    fn retain_blobs(&self, shard: u32, entity: &EntityName, retain: &BTreeSet<crate::BlobId>) {
        self.inner.retain_blobs(shard, entity, retain)
    }

    fn blob_ids(&self, shard: u32, entity: &EntityName) -> Vec<crate::BlobId> {
        self.inner.blob_ids(shard, entity)
    }
}

impl crate::EntityStore for SnapshotAheadOfHeadStore {
    fn store_record(
        &self,
        shard: u32,
        entity: &EntityName,
        after: crate::Seq,
        term: crate::Term,
        records: Vec<Vec<u8>>,
        kind: crate::WriteKind,
    ) -> crate::StoreAck {
        self.inner
            .store_record(shard, entity, after, term, records, kind)
    }

    /// Forwarded whole. `read` feeds recovery and the `ReadReply::head` computation,
    /// which the snapshot's seq participates in — overstating it here would move the
    /// head too, and the disagreement being modelled would vanish.
    fn read(&self, shard: u32, entity: &EntityName) -> crate::ReadReply {
        self.inner.read(shard, entity)
    }

    /// The honest head: this is the authority the overstated snapshot is measured
    /// against (**E3**).
    fn head(&self, shard: u32, entity: &EntityName) -> crate::Seq {
        self.inner.head(shard, entity)
    }

    /// The one method that lies, and the whole point of the double.
    fn snapshot(&self, shard: u32, entity: &EntityName) -> Option<(crate::Seq, Vec<u8>)> {
        let ahead = self.ahead;
        self.inner
            .snapshot(shard, entity)
            .map(|(seq, bytes)| (crate::Seq::new(seq.value() + ahead), bytes))
    }

    fn read_from(
        &self,
        shard: u32,
        entity: &EntityName,
        from: crate::Seq,
        limit: usize,
    ) -> crate::RecordPage {
        self.inner.read_from(shard, entity, from, limit)
    }

    fn prepare(&self, shard: u32, entity: &EntityName, term: crate::Term) -> crate::ReadOutcome {
        self.inner.prepare(shard, entity, term)
    }

    fn store_snapshot(
        &self,
        shard: u32,
        entity: &EntityName,
        at: crate::Seq,
        term: crate::Term,
        state: Vec<u8>,
        kind: crate::WriteKind,
    ) -> crate::StoreAck {
        self.inner
            .store_snapshot(shard, entity, at, term, state, kind)
    }

    fn truncate(&self, shard: u32, entity: &EntityName, after: crate::Seq, term: crate::Term) {
        self.inner.truncate(shard, entity, after, term)
    }

    fn entities(&self, shard: u32) -> Vec<EntityName> {
        self.inner.entities(shard)
    }

    fn seal_range(&self, shard: u32, from: u64) {
        self.inner.seal_range(shard, from)
    }

    fn unseal(&self, shard: u32) {
        self.inner.unseal(shard)
    }

    fn remove_entity(&self, shard: u32, entity: &EntityName) {
        self.inner.remove_entity(shard, entity)
    }

    fn remove_range(&self, shard: u32, from: u64) {
        self.inner.remove_range(shard, from)
    }

    fn drop_shard(&self, shard: u32) {
        self.inner.drop_shard(shard)
    }

    fn shard_bytes(&self, shard: u32) -> u64 {
        self.inner.shard_bytes(shard)
    }
}
