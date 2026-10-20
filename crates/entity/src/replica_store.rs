//! The per-node replica-store actor and the [`ReplicaTransport`] seam (spec storage §5, §6).
//!
//! Durability in the `Quorum` tier is a **per-entity quorum append** (storage §5): the
//! shard leader's [`QuorumReplicator`](crate::replicator::QuorumReplicator) fans an
//! entity's records out to the shard's replicas and reports them durable once a
//! quorum has stored them. This module is the replicas' side of that protocol and
//! the leader's way of reaching them, both built on actor messaging so entity adds
//! no transport (spec §1.1): a per-node [`ReplicaStore`] actor, registered in the
//! receptionist under one key per entity type, owns this node's [`NodeStore`], and
//! [`ActorReplicaTransport`] reaches a replica's store by an ordinary `ask` to that
//! node's actor (local on the leader's own replica, §5.2).

use std::collections::HashMap;
use std::marker::PhantomData;
use std::sync::LazyLock;
use std::sync::Mutex;
use std::time::Duration;

use actor_core::Actor;
use actor_core::ActorRef;
use actor_core::ActorSystem;
use actor_core::BoxFuture;
use actor_core::CallError;
use actor_core::Ctx;
use actor_core::Handler;
use actor_core::HandlerRegistry;
use actor_core::Key;
use actor_core::Manifest;
use actor_core::Message;
use actor_core::NodeId;
use serde::Deserialize;
use serde::Serialize;
use serde_bytes::ByteBuf;

use crate::blobs::BlobId;
use crate::blocking::NodeStore;
use crate::entity::EntityName;
use crate::journal::Seq;
use crate::journal::Term;
use crate::store::BlobAck;
use crate::store::ReadOutcome;
use crate::store::StoreAck;
use crate::store::WriteKind;

/// Per-entity-type interned key strings for the replica store. The receptionist keys
/// purely by string, so the replica store MUST register under a string distinct from
/// the gateway's (which is the bare `entity_type`, §5.3): otherwise a `lookup` would
/// mix the two actor types. We derive `entity.replica/<entity_type>` and intern it
/// (one bounded leak per distinct type, as a runtime type name already permits:
/// `EntityNode::entities_named` takes a `&'static str`).
static REPLICA_KEY_IDS: LazyLock<Mutex<HashMap<&'static str, &'static str>>> =
    LazyLock::new(|| Mutex::new(HashMap::new()));

fn replica_store_key_id(entity_type: &'static str) -> &'static str {
    let mut ids = REPLICA_KEY_IDS.lock().expect("replica key cache poisoned");
    if let Some(id) = ids.get(entity_type) {
        return id;
    }
    let id: &'static str = Box::leak(format!("entity.replica/{entity_type}").into_boxed_str());
    ids.insert(entity_type, id);
    id
}

/// The receptionist key the replica store for an entity type registers under: one
/// well-known key per type (distinct from the gateway's), one entry per node; the
/// replicator looks a replica node's store up here, the way routing looks a gateway
/// up (spec §5.3).
pub(crate) fn replica_store_key<S: ActorSystem>(
    entity_type: &'static str,
) -> Key<ReplicaStore<S>> {
    Key::new(replica_store_key_id(entity_type))
}

/// Quorum-append an entity's records to one replica, fenced by the shard `term`
/// (spec storage §5, §6): [`EntityStore::store_record`](crate::store::EntityStore::store_record)
/// on a peer. The reply is the replica's [`StoreAck`].
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct StoreRecord {
    pub shard: u32,
    pub entity: EntityName,
    pub after: Seq,
    pub term: Term,
    /// Encoded as a sequence of byte strings, so the codec reaches
    /// `serialize_bytes`/`deserialize_bytes` per record instead of serde's default
    /// element-at-a-time sequence path.
    #[serde(with = "byte_strings")]
    pub records: Vec<Vec<u8>>,
    /// A normal append, a recovery write-back (read-repair, storage §6), or a split/merge
    /// transfer copy (storage §8).
    pub kind: WriteKind,
}

impl Message for StoreRecord {
    type Reply = StoreAck;
    const MANIFEST: Manifest = Manifest::new("entity.StoreRecord");
}

/// The fenced recovery read of one replica's view of an entity (spec storage §6):
/// [`EntityStore::prepare`](crate::store::EntityStore::prepare) on a peer. The replica
/// promises not to accept a lower shard term, then returns every occupied slot with
/// its term and the latest snapshot. The reply is a [`ReadOutcome`].
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Prepare {
    pub shard: u32,
    pub entity: EntityName,
    pub term: Term,
}

impl Message for Prepare {
    type Reply = ReadOutcome;
    const MANIFEST: Manifest = Manifest::new("entity.Prepare");
}

/// Quorum-store an entity snapshot to one replica, fenced by the shard `term`
/// (spec storage §9): [`EntityStore::store_snapshot`](crate::store::EntityStore::store_snapshot)
/// on a peer. The reply is the replica's [`StoreAck`]. A [`WriteKind::Transfer`]
/// skips the fence: the split/merge driver landing a moved entity's snapshot under
/// the destination shard's keys (storage §8).
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct StoreSnapshot {
    pub shard: u32,
    pub entity: EntityName,
    pub at: Seq,
    pub term: Term,
    #[serde(with = "serde_bytes")]
    pub state: Vec<u8>,
    pub kind: WriteKind,
}

impl Message for StoreSnapshot {
    type Reply = StoreAck;
    const MANIFEST: Manifest = Manifest::new("entity.StoreSnapshot");
}

/// Tighten one replica's per-shard **append bound** (spec storage §8): durably refuse
/// every future append whose entity hash is `>= from`, at any term. The split
/// (or merge) driver fans this to the shard's replicas and proceeds only once a
/// majority has acked, from then on no append to the moved range can assemble
/// a write quorum, the store half of E15. Idempotent and monotone (the bound
/// only tightens). The ask resolving is the durable acknowledgement.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct SealRange {
    pub shard: u32,
    pub from: u64,
}

impl Message for SealRange {
    type Reply = ();
    const MANIFEST: Manifest = Manifest::new("entity.SealRange");
}

/// Store one immutable, content-addressed blob on a replica (storage §10):
/// [`EntityStore::put_blob`](crate::store::EntityStore::put_blob) on a peer. No
/// `after`, no `term`, no `kind`: a content hash names exactly one byte sequence, so
/// there is nothing to fence or order and no `Fenced`/`Stale` outcome to report. The
/// reply is a [`BlobAck`]: the one thing a replica can still refuse is storing at
/// all, and a quorum that could not distinguish that from success would count a
/// failed replica as a durable copy (**E18**).
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct PutBlob {
    pub shard: u32,
    pub entity: EntityName,
    pub id: BlobId,
    /// One encode on the leader and one decode on every peer, per mebibyte block, on
    /// the create path.
    #[serde(with = "serde_bytes")]
    pub bytes: Vec<u8>,
}

impl Message for PutBlob {
    type Reply = BlobAck;
    const MANIFEST: Manifest = Manifest::new("entity.PutBlob");
}

/// Fetch one blob's bytes from a replica, or `None` if it does not hold it:
/// [`EntityStore::get_blob`](crate::store::EntityStore::get_blob) on a peer. The
/// caller verifies the bytes against the id (B1), so a misdelivery is detectable.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct GetBlob {
    pub shard: u32,
    pub entity: EntityName,
    pub id: BlobId,
}

impl Message for GetBlob {
    /// `ByteBuf` for the same reason `PutBlob::bytes` is bytes: this is the read half
    /// of the same mebibyte, and a restore fetches every block of an image, so the
    /// decode is paid per block there too. [`ReplicaTransport::get_blob`] hands back a
    /// `Vec<u8>`: unwrapping the newtype is a move, not a copy.
    type Reply = Option<ByteBuf>;
    const MANIFEST: Manifest = Manifest::new("entity.GetBlob");
}

/// Whether a replica holds one blob:
/// [`EntityStore::has_blob`](crate::store::EntityStore::has_blob) on a peer.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct HasBlob {
    pub shard: u32,
    pub entity: EntityName,
    pub id: BlobId,
}

impl Message for HasBlob {
    type Reply = bool;
    const MANIFEST: Manifest = Manifest::new("entity.HasBlob");
}

/// Reclaim an entity's blobs on a replica (storage §10): `retain = None` drops the whole
/// area ([`EntityStore::delete_blobs`](crate::store::EntityStore::delete_blobs), on
/// destroy), `retain = Some(ids)` keeps only those
/// ([`EntityStore::retain_blobs`](crate::store::EntityStore::retain_blobs), the
/// mark-from-roots sweep). Idempotent; the ask resolving is the acknowledgement.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct SweepBlobs {
    pub shard: u32,
    pub entity: EntityName,
    pub retain: Option<Vec<BlobId>>,
}

impl Message for SweepBlobs {
    type Reply = ();
    const MANIFEST: Manifest = Manifest::new("entity.SweepBlobs");
}

/// Enumerate every entity a replica holds anything for under one shard:
/// [`EntityStore::entities`](crate::store::EntityStore::entities) on a peer, the
/// migration driver's entity discovery (storage §7). Read from a quorum of the shard's
/// replicas so no committed entity is missed.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ListEntities {
    pub shard: u32,
}

impl Message for ListEntities {
    type Reply = Vec<EntityName>;
    const MANIFEST: Manifest = Manifest::new("entity.ListEntities");
}

/// Enumerate one entity's blob ids on a replica:
/// [`EntityStore::blob_ids`](crate::store::EntityStore::blob_ids) on a peer, the
/// migration driver's source list when copying an entity's blob area (storage §7).
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ListBlobs {
    pub shard: u32,
    pub entity: EntityName,
}

impl Message for ListBlobs {
    type Reply = Vec<BlobId>;
    const MANIFEST: Manifest = Manifest::new("entity.ListBlobs");
}

/// `serde_bytes` for a list of byte strings, which `serde_bytes` itself does not
/// cover: each record is written with `serialize_bytes` and read back through
/// [`ByteBuf`], whose visitor also accepts a plain sequence.
mod byte_strings {
    use serde::Deserialize;
    use serde::Deserializer;
    use serde::Serializer;
    use serde_bytes::ByteBuf;
    use serde_bytes::Bytes;

    pub(super) fn serialize<S: Serializer>(
        records: &[Vec<u8>],
        serializer: S,
    ) -> Result<S::Ok, S::Error> {
        serializer.collect_seq(records.iter().map(|record| Bytes::new(record)))
    }

    pub(super) fn deserialize<'de, D: Deserializer<'de>>(
        deserializer: D,
    ) -> Result<Vec<Vec<u8>>, D::Error> {
        // A `ByteBuf` is a newtype over its allocation, so each `into_vec` moves.
        let records = Vec::<ByteBuf>::deserialize(deserializer)?;
        Ok(records.into_iter().map(ByteBuf::into_vec).collect())
    }
}

/// The node-local replica store for one entity type (spec storage §5): a thin actor over
/// this node's [`NodeStore`], reachable across the cluster so the shard leader's
/// replicator can quorum-append to it and read it back for recovery. One per node
/// per entity type (like the gateway), registered under [`replica_store_key`].
///
/// Parameterized by the **system**, not by the entity type: this actor never folds
/// state, applies an event, or names a facet, so a bare `ActorSystem` is the whole
/// of what it needs: the storage half names the model through `EntityName` alone.
/// Two entity types on one system therefore share this Rust type and are told apart
/// by the key string [`replica_store_key_id`] interns per type. The parameter
/// survives only because [`Actor::System`] must name a system and a struct must use
/// its parameters.
///
/// Every durable write in the cluster that is not the leader's own copy arrives here,
/// so this is the hottest fsync path there is, and why the store is a [`NodeStore`]:
/// its writes run on the node's blocking pool, never on the async worker that is also
/// driving this node's Raft heartbeats (see [`crate::blocking`]).
pub(crate) struct ReplicaStore<S: ActorSystem> {
    store: NodeStore,
    _marker: PhantomData<fn() -> S>,
}

impl<S: ActorSystem> ReplicaStore<S> {
    pub(crate) fn new(store: NodeStore) -> ReplicaStore<S> {
        ReplicaStore {
            store,
            _marker: PhantomData,
        }
    }
}

impl<S: ActorSystem> Actor for ReplicaStore<S> {
    type System = S;

    fn register(registry: &mut HandlerRegistry<ReplicaStore<S>>) {
        registry.accept::<StoreRecord>();
        registry.accept::<Prepare>();
        registry.accept::<StoreSnapshot>();
        registry.accept::<SealRange>();
        registry.accept::<PutBlob>();
        registry.accept::<GetBlob>();
        registry.accept::<HasBlob>();
        registry.accept::<SweepBlobs>();
        registry.accept::<ListEntities>();
        registry.accept::<ListBlobs>();
    }
}

// Every handler's reply IS the peer's acknowledgement, so each returns only what the
// store has already made durable: a reply released early would let the leader count
// this replica toward a quorum for something this node has not stored (**E14**). A
// store write has happened when its future resolves, so awaiting it is the whole of
// that obligation.
impl<S: ActorSystem> Handler<StoreRecord> for ReplicaStore<S> {
    async fn handle(&mut self, msg: StoreRecord, _ctx: &Ctx<ReplicaStore<S>>) -> StoreAck {
        self.store
            .store_record(
                msg.shard,
                &msg.entity,
                msg.after,
                msg.term,
                msg.records,
                msg.kind,
            )
            .await
    }
}

impl<S: ActorSystem> Handler<Prepare> for ReplicaStore<S> {
    async fn handle(&mut self, msg: Prepare, _ctx: &Ctx<ReplicaStore<S>>) -> ReadOutcome {
        self.store.prepare(msg.shard, &msg.entity, msg.term).await
    }
}

impl<S: ActorSystem> Handler<StoreSnapshot> for ReplicaStore<S> {
    async fn handle(&mut self, msg: StoreSnapshot, _ctx: &Ctx<ReplicaStore<S>>) -> StoreAck {
        self.store
            .store_snapshot(
                msg.shard,
                &msg.entity,
                msg.at,
                msg.term,
                msg.state,
                msg.kind,
            )
            .await
    }
}

impl<S: ActorSystem> Handler<SealRange> for ReplicaStore<S> {
    async fn handle(&mut self, msg: SealRange, _ctx: &Ctx<ReplicaStore<S>>) {
        // The bound must be durable before this ask resolves, or a majority could
        // report itself sealed while a restart would forget the promise (**E15**).
        self.store.seal_range(msg.shard, msg.from).await;
    }
}

impl<S: ActorSystem> Handler<PutBlob> for ReplicaStore<S> {
    async fn handle(&mut self, msg: PutBlob, _ctx: &Ctx<ReplicaStore<S>>) -> BlobAck {
        self.store
            .put_blob(msg.shard, &msg.entity, msg.id, msg.bytes)
            .await
    }
}

impl<S: ActorSystem> Handler<GetBlob> for ReplicaStore<S> {
    async fn handle(&mut self, msg: GetBlob, _ctx: &Ctx<ReplicaStore<S>>) -> Option<ByteBuf> {
        self.store
            .get_blob(msg.shard, &msg.entity, msg.id)
            .map(ByteBuf::from)
    }
}

impl<S: ActorSystem> Handler<HasBlob> for ReplicaStore<S> {
    async fn handle(&mut self, msg: HasBlob, _ctx: &Ctx<ReplicaStore<S>>) -> bool {
        self.store.has_blob(msg.shard, &msg.entity, msg.id)
    }
}

impl<S: ActorSystem> Handler<SweepBlobs> for ReplicaStore<S> {
    async fn handle(&mut self, msg: SweepBlobs, _ctx: &Ctx<ReplicaStore<S>>) {
        match msg.retain {
            None => self.store.delete_blobs(msg.shard, &msg.entity).await,
            Some(ids) => {
                self.store
                    .retain_blobs(msg.shard, &msg.entity, ids.into_iter().collect())
                    .await
            }
        }
    }
}

impl<S: ActorSystem> Handler<ListEntities> for ReplicaStore<S> {
    async fn handle(&mut self, msg: ListEntities, _ctx: &Ctx<ReplicaStore<S>>) -> Vec<EntityName> {
        self.store.entities(msg.shard)
    }
}

impl<S: ActorSystem> Handler<ListBlobs> for ReplicaStore<S> {
    async fn handle(&mut self, msg: ListBlobs, _ctx: &Ctx<ReplicaStore<S>>) -> Vec<BlobId> {
        self.store.blob_ids(msg.shard, &msg.entity)
    }
}

/// How the replicator reaches a shard's replica stores (spec storage §3.3, §5): one method per
/// replica-store message, each sending it to `node`'s store and resolving with its
/// reply, or a [`CallError`] if no reply arrived `within` the deadline. The methods
/// are named for the [`EntityStore`](crate::store::EntityStore) call each runs on the
/// peer. Object-safe, so the journal stays generic over just the consensus type `R`
/// and names no system type at all: the typed [`ReplicaStore`] ref and its
/// receptionist key live behind this seam, built in `entities_named` where the
/// system is known.
pub trait ReplicaTransport: Send + Sync + 'static {
    fn store_record(
        &self,
        node: NodeId,
        msg: StoreRecord,
        within: Duration,
    ) -> BoxFuture<'static, Result<StoreAck, CallError>>;

    fn prepare(
        &self,
        node: NodeId,
        msg: Prepare,
        within: Duration,
    ) -> BoxFuture<'static, Result<ReadOutcome, CallError>>;

    fn store_snapshot(
        &self,
        node: NodeId,
        msg: StoreSnapshot,
        within: Duration,
    ) -> BoxFuture<'static, Result<StoreAck, CallError>>;

    /// The ask resolving means the bound is durable on `node` (**E15**).
    fn seal_range(
        &self,
        node: NodeId,
        msg: SealRange,
        within: Duration,
    ) -> BoxFuture<'static, Result<(), CallError>>;

    /// The [`BlobAck`] distinguishes a stored copy from a replica that could not
    /// store it, so only real copies count toward the quorum (**E18**).
    fn put_blob(
        &self,
        node: NodeId,
        msg: PutBlob,
        within: Duration,
    ) -> BoxFuture<'static, Result<BlobAck, CallError>>;

    fn get_blob(
        &self,
        node: NodeId,
        msg: GetBlob,
        within: Duration,
    ) -> BoxFuture<'static, Result<Option<Vec<u8>>, CallError>>;

    fn has_blob(
        &self,
        node: NodeId,
        msg: HasBlob,
        within: Duration,
    ) -> BoxFuture<'static, Result<bool, CallError>>;

    fn sweep_blobs(
        &self,
        node: NodeId,
        msg: SweepBlobs,
        within: Duration,
    ) -> BoxFuture<'static, Result<(), CallError>>;

    fn entities(
        &self,
        node: NodeId,
        msg: ListEntities,
        within: Duration,
    ) -> BoxFuture<'static, Result<Vec<EntityName>, CallError>>;

    fn blob_ids(
        &self,
        node: NodeId,
        msg: ListBlobs,
        within: Duration,
    ) -> BoxFuture<'static, Result<Vec<BlobId>, CallError>>;

    /// Launch a detached background task (spec storage §5). The replicator uses it to drain
    /// the straggler peer asks of an append that already committed on a quorum, so the
    /// commit returns at quorum latency while every issued ask still runs to
    /// completion: its `AskIssued`/`AskOutcome` bracket closes, preserving
    /// no-silent-loss (§10). Backed by the system's `launch` (actor §11.1).
    fn launch(&self, task: BoxFuture<'static, ()>);
}

/// The actor-messaging [`ReplicaTransport`] (spec §1.1: no new transport): it
/// resolves a node's [`ReplicaStore`] in the receptionist and `ask`s it. A store on
/// this node resolves to the local actor, so the leader's append to its own replica
/// is a local call with no serialization (§5.2). Resolution is a local receptionist
/// read each call, never stale across a peer restart (a restarted node re-registers a
/// fresh ref).
pub(crate) struct ActorReplicaTransport<S: ActorSystem> {
    system: S,
    entity_type: &'static str,
}

impl<S: ActorSystem> ActorReplicaTransport<S> {
    pub(crate) fn new(system: S, entity_type: &'static str) -> ActorReplicaTransport<S> {
        ActorReplicaTransport {
            system,
            entity_type,
        }
    }

    /// The replica store registered on `node`, if discovered (spec §5.3).
    fn resolve(&self, node: NodeId) -> Option<ActorRef<ReplicaStore<S>>> {
        self.system
            .receptionist()
            .lookup(replica_store_key::<S>(self.entity_type))
            .into_vec()
            .into_iter()
            .find(|store| store.id().node() == node)
    }

    /// Resolve `node`'s replica store and `ask` it, or `Unreachable` if it is not
    /// discovered: every [`ReplicaTransport`] method is this (storage §5).
    fn ask<M>(
        &self,
        node: NodeId,
        msg: M,
        within: Duration,
    ) -> BoxFuture<'static, Result<M::Reply, CallError>>
    where
        M: Message,
        ReplicaStore<S>: Handler<M>,
    {
        let store = self.resolve(node);
        Box::pin(async move {
            store
                .ok_or(CallError::Unreachable)?
                .ask_timeout(msg, within)
                .await
        })
    }
}

impl<S: ActorSystem> ReplicaTransport for ActorReplicaTransport<S> {
    fn store_record(
        &self,
        node: NodeId,
        msg: StoreRecord,
        within: Duration,
    ) -> BoxFuture<'static, Result<StoreAck, CallError>> {
        self.ask(node, msg, within)
    }

    fn prepare(
        &self,
        node: NodeId,
        msg: Prepare,
        within: Duration,
    ) -> BoxFuture<'static, Result<ReadOutcome, CallError>> {
        self.ask(node, msg, within)
    }

    fn store_snapshot(
        &self,
        node: NodeId,
        msg: StoreSnapshot,
        within: Duration,
    ) -> BoxFuture<'static, Result<StoreAck, CallError>> {
        self.ask(node, msg, within)
    }

    fn seal_range(
        &self,
        node: NodeId,
        msg: SealRange,
        within: Duration,
    ) -> BoxFuture<'static, Result<(), CallError>> {
        self.ask(node, msg, within)
    }

    fn put_blob(
        &self,
        node: NodeId,
        msg: PutBlob,
        within: Duration,
    ) -> BoxFuture<'static, Result<BlobAck, CallError>> {
        self.ask(node, msg, within)
    }

    fn get_blob(
        &self,
        node: NodeId,
        msg: GetBlob,
        within: Duration,
    ) -> BoxFuture<'static, Result<Option<Vec<u8>>, CallError>> {
        let reply = self.ask(node, msg, within);
        Box::pin(async move { reply.await.map(|found| found.map(ByteBuf::into_vec)) })
    }

    fn has_blob(
        &self,
        node: NodeId,
        msg: HasBlob,
        within: Duration,
    ) -> BoxFuture<'static, Result<bool, CallError>> {
        self.ask(node, msg, within)
    }

    fn sweep_blobs(
        &self,
        node: NodeId,
        msg: SweepBlobs,
        within: Duration,
    ) -> BoxFuture<'static, Result<(), CallError>> {
        self.ask(node, msg, within)
    }

    fn entities(
        &self,
        node: NodeId,
        msg: ListEntities,
        within: Duration,
    ) -> BoxFuture<'static, Result<Vec<EntityName>, CallError>> {
        self.ask(node, msg, within)
    }

    fn blob_ids(
        &self,
        node: NodeId,
        msg: ListBlobs,
        within: Duration,
    ) -> BoxFuture<'static, Result<Vec<BlobId>, CallError>> {
        self.ask(node, msg, within)
    }

    fn launch(&self, task: BoxFuture<'static, ()>) {
        self.system.launch(task);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use actor_serialization::JsonCodec;
    use actor_serialization::PostcardCodec;

    /// The shape `StoreRecord` had when its field was a list of `ByteBuf`s: the
    /// encoding `byte_strings` must reproduce byte for byte.
    #[derive(Serialize)]
    struct ByteBufRecords {
        shard: u32,
        entity: EntityName,
        after: Seq,
        term: Term,
        records: Vec<ByteBuf>,
        kind: WriteKind,
    }

    #[test]
    fn records_travel_as_a_list_of_byte_strings_under_every_codec() {
        let msg = StoreRecord {
            shard: 3,
            entity: EntityName::new("t", "k"),
            after: Seq::new(1),
            term: Term::new(2),
            records: vec![vec![1, 2, 3], Vec::new(), vec![255]],
            kind: WriteKind::Repair,
        };
        let mirror = ByteBufRecords {
            shard: msg.shard,
            entity: msg.entity.clone(),
            after: msg.after,
            term: msg.term,
            records: msg.records.iter().cloned().map(ByteBuf::from).collect(),
            kind: msg.kind,
        };
        for codec in [
            &JsonCodec as &dyn actor_serialization::Codec,
            &PostcardCodec,
        ] {
            let bytes = actor_serialization::encode(codec, &msg).unwrap();
            assert_eq!(
                bytes,
                actor_serialization::encode(codec, &mirror).unwrap(),
                "{}: the record list changed its encoding",
                codec.name(),
            );
            let back: StoreRecord = actor_serialization::decode(codec, &bytes).unwrap();
            assert_eq!(back.records, msg.records, "{}", codec.name());
        }
    }
}
