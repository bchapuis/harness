//! Shared simulation invariants over grain events (granary spec §7, spec §18.5).
//!
//! These are safety predicates every grain-backed swarm wants: commits advance,
//! and a grain is live at most once per node. They are claims about *granary's*
//! contract, not about any one suite, so they live here rather than in each test
//! binary.
//!
//! Each is constructed with the label it reports under, so a suite still names
//! its own violations: `machine-commit-monotonic` and `disk-grain-commit-monotonic`
//! are the same predicate observed from different workloads.
//!
//! Behind the `testing` feature: test support, which should not ship in a
//! production build of the crate.

use std::collections::BTreeMap;
use std::collections::BTreeSet;

use actor_core::Event;
use actor_core::NodeId;
use actor_simulation::Invariant;

use crate::GrainEvent;
use crate::GrainName;

/// **Commit monotonicity** (invariants **G3**/**G5**): a grain's committed
/// sequence strictly increases.
///
/// A commit at a sequence at or below the current head means two writers both
/// believed themselves authoritative — a minority "leader" that committed, or a
/// replayed entry accepted twice. Either is a split of the commit log.
pub struct CommitMonotonic {
    label: &'static str,
    noun: &'static str,
    last: BTreeMap<GrainName, u64>,
}

impl CommitMonotonic {
    /// Observe under `label`, describing the subject as `noun` in violations —
    /// "grain" for a plain grain suite, "machine" where the grain is a machine.
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
        if let Some(GrainEvent::Committed { name, seq, .. }) = event.as_app::<GrainEvent>() {
            let prev = self.last.get(name).copied().unwrap_or(0);
            if *seq <= prev {
                return Err(format!(
                    "{} {name} committed seq {seq} not after previous head {prev} (G3/G5)",
                    self.noun,
                ));
            }
            self.last.insert(name.clone(), *seq);
        }
        Ok(())
    }
}

/// **Exactly-once activation per node** (invariant **G6**): on any one node, a
/// grain is never live twice at once.
///
/// Keyed by `(node, name)`, so an activation that migrates to another leader on
/// failover is not mistaken for a second one. Crash-sound: a node's live set is
/// cleared when the stream reports that node `NodeDown` — its activations are
/// gone with it — so a re-activation after the node rejoins and re-leads is not
/// a false positive.
///
/// **Not** [`actor_simulation::SingletonAtMostOnePerNode`], despite the similar
/// name: that one is the *cluster-utilities* singleton (U2), keyed off
/// `Event::SingletonStarted`. This is the grain analogue, keyed off
/// `GrainEvent::Activated`.
pub struct ActivationSingletonPerNode {
    label: &'static str,
    noun: &'static str,
    live: BTreeSet<(NodeId, GrainName)>,
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
        // re-activation on the recovered node is sound (G6 is per live node).
        if let Event::NodeDown { node, .. } = event {
            self.live.retain(|(n, _)| n != node);
            return Ok(());
        }
        match event.as_app::<GrainEvent>() {
            Some(GrainEvent::Activated { node, name }) => {
                let fresh = self.live.insert((*node, name.clone()));
                if !fresh {
                    return Err(format!(
                        "{} {name} activated while already live on {node} (G6)",
                        self.noun,
                    ));
                }
            }
            Some(GrainEvent::Passivated { node, name }) => {
                self.live.remove(&(*node, name.clone()));
            }
            _ => {}
        }
        Ok(())
    }

    fn forget_node(&mut self, node: NodeId) {
        // The other way a node's activations vanish: its process ended. Nothing
        // emits `Passivated` for an activation that died with its host, and the
        // successor may legitimately re-activate it (G6 is per *live* node).
        self.live.retain(|(n, _)| *n != node);
    }
}

/// A [`GrainStore`](crate::GrainStore) with a fixed answer: every write returns
/// `refusal`, every read is empty, every enumeration is empty.
///
/// The deterministic stand-in for a store that never acknowledges — an index whose
/// registrations always fail, a replica permanently fenced — where a real store
/// would need a fault injector to reach the same state. `StaticGrainStore::fenced()`
/// is the common case: writes refused at an unreachably high term.
pub struct StaticGrainStore {
    refusal: crate::StoreAck,
}

impl StaticGrainStore {
    /// A store whose every write is refused with `refusal`.
    pub fn new(refusal: crate::StoreAck) -> StaticGrainStore {
        StaticGrainStore { refusal }
    }

    /// A store fenced at an unreachably high term, so nothing hosted on it can
    /// commit while reads still succeed — activation works and the failure surfaces
    /// where it does in production, at the commit.
    pub fn fenced() -> StaticGrainStore {
        StaticGrainStore::new(crate::StoreAck::Fenced(crate::Term::new(u64::MAX)))
    }

    /// A factory handing this store to every node, for
    /// [`GranaryConfig::grain_store`](crate::GranaryConfig).
    pub fn factory(refusal: crate::StoreAck) -> crate::GrainStoreFactory {
        std::sync::Arc::new(move |_| {
            std::sync::Arc::new(StaticGrainStore::new(refusal.clone()))
                as std::sync::Arc<dyn crate::GrainStore>
        })
    }

    fn empty_reply() -> crate::ReadReply {
        crate::ReadReply {
            slots: Vec::new(),
            snapshot: None,
        }
    }
}

impl crate::GrainBlobStore for StaticGrainStore {
    fn put_blob(
        &self,
        _shard: u32,
        _grain: &GrainName,
        _id: crate::BlobId,
        _bytes: Vec<u8>,
    ) -> crate::Reserved<()> {
        crate::Reserved::ready(())
    }

    fn get_blob(
        &self,
        _shard: u32,
        _grain: &GrainName,
        _id: crate::BlobId,
    ) -> crate::Reserved<Option<Vec<u8>>> {
        crate::Reserved::ready(None)
    }

    fn has_blob(
        &self,
        _shard: u32,
        _grain: &GrainName,
        _id: crate::BlobId,
    ) -> crate::Reserved<bool> {
        crate::Reserved::ready(false)
    }

    fn delete_blob(
        &self,
        _shard: u32,
        _grain: &GrainName,
        _id: crate::BlobId,
    ) -> crate::Reserved<()> {
        crate::Reserved::ready(())
    }

    fn delete_blobs(&self, _shard: u32, _grain: &GrainName) -> crate::Reserved<()> {
        crate::Reserved::ready(())
    }

    fn retain_blobs(
        &self,
        _shard: u32,
        _grain: &GrainName,
        _retain: &BTreeSet<crate::BlobId>,
    ) -> crate::Reserved<()> {
        crate::Reserved::ready(())
    }

    fn blob_ids(&self, _shard: u32, _grain: &GrainName) -> crate::Reserved<Vec<crate::BlobId>> {
        crate::Reserved::ready(Vec::new())
    }
}

impl crate::GrainStore for StaticGrainStore {
    fn store_record(
        &self,
        _shard: u32,
        _grain: &GrainName,
        _after: crate::Seq,
        _term: crate::Term,
        _records: Vec<Vec<u8>>,
        _kind: crate::WriteKind,
    ) -> crate::Reserved<crate::StoreAck> {
        crate::Reserved::ready(self.refusal.clone())
    }

    fn read(&self, _shard: u32, _grain: &GrainName) -> crate::Reserved<crate::ReadReply> {
        crate::Reserved::ready(StaticGrainStore::empty_reply())
    }

    fn read_from(
        &self,
        _shard: u32,
        _grain: &GrainName,
        _from: crate::Seq,
        _limit: usize,
    ) -> crate::Reserved<Vec<(crate::Seq, Vec<u8>)>> {
        crate::Reserved::ready(Vec::new())
    }

    fn prepare(
        &self,
        _shard: u32,
        _grain: &GrainName,
        _term: crate::Term,
    ) -> crate::Reserved<crate::ReadOutcome> {
        // Prepared, not fenced: a read must succeed so activation gets far enough
        // for the refusal to land at the commit.
        crate::Reserved::ready(crate::ReadOutcome::Prepared(StaticGrainStore::empty_reply()))
    }

    fn store_snapshot(
        &self,
        _shard: u32,
        _grain: &GrainName,
        _at: crate::Seq,
        _term: crate::Term,
        _state: Vec<u8>,
        _kind: crate::WriteKind,
    ) -> crate::Reserved<crate::StoreAck> {
        crate::Reserved::ready(self.refusal.clone())
    }

    fn truncate(
        &self,
        _shard: u32,
        _grain: &GrainName,
        _after: crate::Seq,
        _term: crate::Term,
    ) -> crate::Reserved<()> {
        crate::Reserved::ready(())
    }

    fn grains(&self, _shard: u32) -> Vec<GrainName> {
        Vec::new()
    }

    fn seal_range(&self, _shard: u32, _from: u64) -> crate::Reserved<()> {
        crate::Reserved::ready(())
    }

    fn unseal(&self, _shard: u32) -> crate::Reserved<()> {
        crate::Reserved::ready(())
    }

    fn remove_grain(&self, _shard: u32, _grain: &GrainName) -> crate::Reserved<()> {
        crate::Reserved::ready(())
    }

    fn remove_range(&self, _shard: u32, _from: u64) -> crate::Reserved<()> {
        crate::Reserved::ready(())
    }

    fn drop_shard(&self, _shard: u32) -> crate::Reserved<()> {
        crate::Reserved::ready(())
    }

    fn shard_bytes(&self, _shard: u32) -> u64 {
        0
    }
}
