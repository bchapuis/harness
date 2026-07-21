//! The per-node `BlobReplica` actor and the [`BlobTransport`] seam (spec §6).
//!
//! The `Clustered` tier reuses the actor framework's transport, with no new wire
//! protocol (actor §2.2), exactly as the grain Quorum replicator does (granary
//! §7.2) — but minus everything fencing- and order-related. A per-node
//! [`BlobReplica`] actor owns this node's on-disk [`LocalBlobStore`] and accepts
//! four messages: [`StoreBlob`], [`FetchBlob`], [`HasBlob`], and
//! [`DeleteNamespace`]. It is registered in the receptionist under one well-known
//! key ([`blob_replica_key`]) so peers discover it, and [`ActorBlobTransport`]
//! reaches a peer's replica by an ordinary `ask`.
//!
//! Unlike granary's `StoreRecord`, [`StoreBlob`] carries **no shard, no `after`,
//! no term, and no `repair` flag** (spec §6): nothing needs fencing and nothing
//! needs ordering, so the only field beyond the bytes is the namespace the blob
//! lives under. This message set makes the spec §4 thesis concrete in the wire
//! contract: a verified write, a verified read, and a monotonic delete flag, with
//! no order and no term.

use std::marker::PhantomData;
use std::ops::Range;
use std::time::Duration;

use actor_core::Actor;
use actor_core::ActorRef;
use actor_core::BoxFuture;
use actor_core::CallError;
use actor_core::Ctx;
use actor_core::Handler;
use actor_core::HandlerRegistry;
use actor_core::Manifest;
use actor_core::Message;
use actor_core::NodeId;
use actor_core::receptionist::Key;
use serde::Deserialize;
use serde::Serialize;

use crate::blob::BlobError;
use crate::blob::BlobId;
use crate::blob::Namespace;
use crate::event::BlobEvent;
use crate::local::LocalBlobStore;
use crate::system::BlobSystem;
use crate::tombstone::Tombstone;
use crate::tombstone::TombstoneSet;

/// The receptionist key every node's [`BlobReplica`] registers under: one
/// well-known key, one entry per node (spec §6). Unlike granary's per-grain-type
/// key, a blob store has a single replica type, so the key is a constant. The
/// transport looks a peer's replica up here, then `ask`s it.
pub fn blob_replica_key<S: BlobSystem>() -> Key<BlobReplica<S>> {
    Key::new("blob.replica")
}

/// Store one blob's bytes on a replica under `ns` (spec §6). The reply is a
/// [`StoreAck`]. No shard, no `after`, no term, no `repair`: a content hash names
/// exactly one byte sequence, so there is nothing to fence and nothing to order
/// (spec §4).
#[derive(Serialize, Deserialize)]
pub struct StoreBlob {
    pub ns: Namespace,
    pub id: BlobId,
    pub bytes: Vec<u8>,
}

impl Message for StoreBlob {
    type Reply = StoreAck;
    const MANIFEST: Manifest = Manifest::new("blob.StoreBlob");
}

/// Fetch one blob's **whole** raw bytes from a replica (spec §6). The reply is
/// `Some(bytes)` if the replica holds it, else `None` (absent or the namespace is
/// tombstoned). The bytes are **not** verified by the replica — the caller
/// re-hashes them after transfer (spec §4, §5.2, **B1**), so a non-verifying copy
/// is distinguishable from an absent one. `range` is reserved for range-verified
/// streaming (spec §10); v1 returns the whole blob, because a range cannot be
/// verified against the id without it.
#[derive(Serialize, Deserialize)]
pub struct FetchBlob {
    pub ns: Namespace,
    pub id: BlobId,
    pub range: Option<Range<u64>>,
}

impl Message for FetchBlob {
    type Reply = Option<Vec<u8>>;
    const MANIFEST: Manifest = Manifest::new("blob.FetchBlob");
}

/// Whether a replica holds `(ns, id)` (spec §6) — the per-owner probe that gates
/// a reconcile copy (spec §7). A tombstoned namespace reports `false`.
#[derive(Serialize, Deserialize)]
pub struct HasBlob {
    pub ns: Namespace,
    pub id: BlobId,
}

impl Message for HasBlob {
    type Reply = bool;
    const MANIFEST: Manifest = Manifest::new("blob.HasBlob");
}

/// Record a namespace tombstone on a replica and sweep its local bytes (spec §5.3,
/// §6). Idempotent and monotonic: a redelivered, gossiped, or reconcile-driven
/// delete is harmless. The reply is a [`DeleteAck`]. `deleted_at` is the
/// anchoring stamp carried for diagnostics and forward compatibility; tombstone
/// *presence*, not its stamp, makes a namespace gone (spec §5.3).
#[derive(Serialize, Deserialize)]
pub struct DeleteNamespace {
    pub ns: Namespace,
    pub deleted_at: u64,
}

impl Message for DeleteNamespace {
    type Reply = DeleteAck;
    const MANIFEST: Manifest = Manifest::new("blob.DeleteNamespace");
}

/// Ask a replica for its whole tombstone awareness set (spec §5.3) — the
/// **anti-entropy pull** the reconcile loop runs each pass so a node that was
/// partitioned or down across a `delete_namespace` re-syncs the tombstones it
/// missed *before* it serves or re-replicates a stale blob (**B7**). The initial
/// `delete_namespace` fan-out (spec §5.3) reaches every *then-serving* node; this
/// pull is the "thereafter by gossip" / "re-syncs the set from the anchor owners
/// on rejoin" half, without which a rejoining holder resurrects the namespace.
#[derive(Serialize, Deserialize)]
pub struct SyncTombstones;

impl Message for SyncTombstones {
    type Reply = Vec<Tombstone>;
    const MANIFEST: Manifest = Manifest::new("blob.SyncTombstones");
}

/// A replica's response to [`StoreBlob`] (spec §6). It has **no** `Fenced` or
/// `Stale` variant (contrast granary's `StoreAck`): there is no term and no
/// mutable head to be stale against.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum StoreAck {
    /// The blob is durably stored on this replica (or was already present, B2).
    Stored,
    /// The target namespace is tombstoned, so the store was refused (spec §5.3).
    Deleted,
}

/// A replica's response to [`DeleteNamespace`] (spec §6): the tombstone is durably
/// recorded. A named unit type (rather than `()`) keeps the wire contract explicit
/// and leaves room to grow.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum DeleteAck {
    /// The tombstone is durably recorded on this replica; its bytes are swept.
    Acked,
}

/// The node-local replica store (spec §6): a thin actor over this node's
/// [`LocalBlobStore`], reachable across the cluster so the `Clustered` tier can
/// replicate to it, read it back, probe it, and tombstone it. One per node,
/// registered under [`blob_replica_key`].
///
/// It also updates this node's in-memory [`TombstoneSet`] on a `DeleteNamespace`,
/// so the tier shares one source of delete-awareness with the replica: the
/// on-disk tombstone (which refuses stores and survives restart) and the in-memory
/// set (which the tier's `get`/`put` consult and gossip) move together (spec §5.3).
pub struct BlobReplica<S: BlobSystem> {
    store: LocalBlobStore,
    tombstones: TombstoneSet,
    _marker: PhantomData<fn() -> S>,
}

impl<S: BlobSystem> BlobReplica<S> {
    /// Wrap this node's on-disk store as a replica actor, updating `tombstones`
    /// (the node's cluster-wide awareness set) alongside the durable store.
    pub fn new(store: LocalBlobStore, tombstones: TombstoneSet) -> BlobReplica<S> {
        BlobReplica {
            store,
            tombstones,
            _marker: PhantomData,
        }
    }
}

impl<S: BlobSystem> Actor for BlobReplica<S> {
    type System = S;

    fn register(registry: &mut HandlerRegistry<BlobReplica<S>>) {
        registry.accept::<StoreBlob>();
        registry.accept::<FetchBlob>();
        registry.accept::<HasBlob>();
        registry.accept::<DeleteNamespace>();
        registry.accept::<SyncTombstones>();
    }
}

impl<S: BlobSystem> Handler<StoreBlob> for BlobReplica<S> {
    async fn handle(&mut self, msg: StoreBlob, ctx: &Ctx<BlobReplica<S>>) -> StoreAck {
        match self.store.store(&msg.ns, &msg.id, &msg.bytes) {
            Ok(()) => {
                ctx.system().emit_blob_event(BlobEvent::Stored {
                    node: ctx.system().node(),
                    ns: msg.ns,
                    id: msg.id,
                });
                StoreAck::Stored
            }
            Err(BlobError::Deleted(_)) => StoreAck::Deleted,
            // A non-tombstone store failure means this node's disk is broken; a
            // replica cannot function without durable storage (as granary's
            // `FileGrainStore` panics on store failure). The panic stops the actor,
            // its registration is pruned, and reconcile re-replicates the blob to a
            // healthy owner (B6) — so the failure surfaces to the caller as a
            // non-ack and the put falls back to another owner.
            Err(err) => panic!("blob replica failed to store {}: {err}", msg.id),
        }
    }
}

impl<S: BlobSystem> Handler<FetchBlob> for BlobReplica<S> {
    async fn handle(&mut self, msg: FetchBlob, _ctx: &Ctx<BlobReplica<S>>) -> Option<Vec<u8>> {
        // Raw, unverified bytes: the caller verifies after transfer (B1, §5.2).
        self.store.read_raw(&msg.ns, &msg.id)
    }
}

impl<S: BlobSystem> Handler<HasBlob> for BlobReplica<S> {
    async fn handle(&mut self, msg: HasBlob, _ctx: &Ctx<BlobReplica<S>>) -> bool {
        self.store.present(&msg.ns, &msg.id)
    }
}

impl<S: BlobSystem> Handler<DeleteNamespace> for BlobReplica<S> {
    async fn handle(&mut self, msg: DeleteNamespace, ctx: &Ctx<BlobReplica<S>>) -> DeleteAck {
        // Durable tombstone + sweep first (refuses later stores, survives restart),
        // then in-memory awareness — both monotonic, so a redelivered, gossiped, or
        // reconcile-driven delete is harmless (spec §5.3, §6).
        self.store
            .tombstone(&msg.ns, msg.deleted_at)
            .expect("blob replica failed to record a namespace tombstone");
        self.tombstones.insert(&msg.ns, msg.deleted_at);
        ctx.system().emit_blob_event(BlobEvent::Tombstoned {
            node: ctx.system().node(),
            ns: msg.ns,
        });
        DeleteAck::Acked
    }
}

impl<S: BlobSystem> Handler<SyncTombstones> for BlobReplica<S> {
    async fn handle(&mut self, _msg: SyncTombstones, _ctx: &Ctx<BlobReplica<S>>) -> Vec<Tombstone> {
        // The full awareness set, for a peer's reconcile-pass anti-entropy pull
        // (spec §5.3, B7). Cheap: one tiny record per deleted namespace.
        self.tombstones.snapshot()
    }
}

/// The boxed result of a [`BlobTransport::store_blob`] — the future the
/// `Clustered` tier collects for its `W`-of-`R` quorum and drains in the
/// background (spec §5.2).
pub type StoreAckFuture = BoxFuture<'static, Result<StoreAck, CallError>>;

/// How the `Clustered` tier reaches a node's [`BlobReplica`] (spec §6). Object-safe
/// (the analogue of granary's `ReplicaTransport`), so the tier and the reconcile
/// loop stay free of the system's `Clock`/`Entropy`/`Spawner`/`Transport` type
/// parameters; keeping it a seam preserves deterministic simulation (spec §8).
pub trait BlobTransport: Send + Sync + 'static {
    /// `StoreBlob` to `node`'s replica (spec §5.2). A `Stored` ack counts toward
    /// the `W` durability target; a `Deleted` ack surfaces the tombstone.
    fn store_blob(
        &self,
        node: NodeId,
        ns: Namespace,
        id: BlobId,
        bytes: Vec<u8>,
        within: Duration,
    ) -> StoreAckFuture;

    /// `FetchBlob` from `node`'s replica (spec §5.2): the whole raw bytes, or
    /// `None`. The caller verifies (B1).
    fn fetch_blob(
        &self,
        node: NodeId,
        ns: Namespace,
        id: BlobId,
        range: Option<Range<u64>>,
        within: Duration,
    ) -> BoxFuture<'static, Result<Option<Vec<u8>>, CallError>>;

    /// `HasBlob` on `node`'s replica (spec §7) — gates a reconcile copy.
    fn has_blob(
        &self,
        node: NodeId,
        ns: Namespace,
        id: BlobId,
        within: Duration,
    ) -> BoxFuture<'static, Result<bool, CallError>>;

    /// `DeleteNamespace` on `node`'s replica (spec §5.3): record the tombstone and
    /// sweep. Fanned out to every serving node, not only a blob's owners.
    fn delete_namespace(
        &self,
        node: NodeId,
        ns: Namespace,
        deleted_at: u64,
        within: Duration,
    ) -> BoxFuture<'static, Result<DeleteAck, CallError>>;

    /// `SyncTombstones` on `node`'s replica (spec §5.3, **B7**): pull its awareness
    /// set so a rejoining or lagging node learns the deletes it missed, the
    /// "thereafter by gossip" half of tombstone dissemination.
    fn sync_tombstones(
        &self,
        node: NodeId,
        within: Duration,
    ) -> BoxFuture<'static, Result<Vec<Tombstone>, CallError>>;

    /// Launch a detached background task (spec §5.2, §7): draining the straggler
    /// stores of a `put` that already reached `W`, so the commit returns at `W`
    /// latency while every issued ask still runs to completion.
    fn launch(&self, task: BoxFuture<'static, ()>);
}

/// The actor-messaging [`BlobTransport`] (spec §6: no new transport): it resolves a
/// node's [`BlobReplica`] in the receptionist and `ask`s it. A replica on this node
/// resolves to the local actor, so an owner's store to itself is a local call with
/// no serialization (spec §5.2). Resolution is a local receptionist read each call
/// — cheap, and never stale across a peer restart (a restarted node re-registers a
/// fresh ref).
pub struct ActorBlobTransport<S: BlobSystem> {
    system: S,
}

impl<S: BlobSystem> ActorBlobTransport<S> {
    /// Build the transport over `system`'s receptionist and spawner.
    pub fn new(system: S) -> ActorBlobTransport<S> {
        ActorBlobTransport { system }
    }

    /// The replica registered on `node`, if discovered (spec §6).
    fn resolve(&self, node: NodeId) -> Option<ActorRef<BlobReplica<S>>> {
        self.system
            .receptionist()
            .lookup(blob_replica_key::<S>())
            .into_vec()
            .into_iter()
            .find(|replica| replica.id().node() == node)
    }
}

impl<S: BlobSystem> BlobTransport for ActorBlobTransport<S> {
    fn store_blob(
        &self,
        node: NodeId,
        ns: Namespace,
        id: BlobId,
        bytes: Vec<u8>,
        within: Duration,
    ) -> StoreAckFuture {
        let replica = self.resolve(node);
        Box::pin(async move {
            let replica = replica.ok_or(CallError::Unreachable)?;
            replica
                .ask_timeout(StoreBlob { ns, id, bytes }, within)
                .await
        })
    }

    fn fetch_blob(
        &self,
        node: NodeId,
        ns: Namespace,
        id: BlobId,
        range: Option<Range<u64>>,
        within: Duration,
    ) -> BoxFuture<'static, Result<Option<Vec<u8>>, CallError>> {
        let replica = self.resolve(node);
        Box::pin(async move {
            let replica = replica.ok_or(CallError::Unreachable)?;
            replica
                .ask_timeout(FetchBlob { ns, id, range }, within)
                .await
        })
    }

    fn has_blob(
        &self,
        node: NodeId,
        ns: Namespace,
        id: BlobId,
        within: Duration,
    ) -> BoxFuture<'static, Result<bool, CallError>> {
        let replica = self.resolve(node);
        Box::pin(async move {
            let replica = replica.ok_or(CallError::Unreachable)?;
            replica.ask_timeout(HasBlob { ns, id }, within).await
        })
    }

    fn delete_namespace(
        &self,
        node: NodeId,
        ns: Namespace,
        deleted_at: u64,
        within: Duration,
    ) -> BoxFuture<'static, Result<DeleteAck, CallError>> {
        let replica = self.resolve(node);
        Box::pin(async move {
            let replica = replica.ok_or(CallError::Unreachable)?;
            replica
                .ask_timeout(DeleteNamespace { ns, deleted_at }, within)
                .await
        })
    }

    fn sync_tombstones(
        &self,
        node: NodeId,
        within: Duration,
    ) -> BoxFuture<'static, Result<Vec<Tombstone>, CallError>> {
        let replica = self.resolve(node);
        Box::pin(async move {
            let replica = replica.ok_or(CallError::Unreachable)?;
            replica.ask_timeout(SyncTombstones, within).await
        })
    }

    fn launch(&self, task: BoxFuture<'static, ()>) {
        self.system.launch(task);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use actor_core::ActorSystem;
    use actor_core::LocalSystemBuilder;
    use actor_simulation::Simulation;

    fn round_trip<T>(value: &T) -> T
    where
        T: Serialize + serde::de::DeserializeOwned,
    {
        // The replica messages cross node boundaries, so each must survive a codec
        // round-trip (V&V checklist #1). serde_json stands in for the wire codec;
        // the cross-node encoding is exercised end-to-end by the cluster sim.
        let bytes = serde_json::to_vec(value).expect("serialize");
        serde_json::from_slice(&bytes).expect("deserialize")
    }

    #[test]
    fn replica_messages_and_acks_round_trip() {
        let ns = Namespace::new(b"ns".to_vec());
        let id = BlobId::of(b"block");
        assert_eq!(
            round_trip(&StoreBlob {
                ns: ns.clone(),
                id,
                bytes: b"block".to_vec()
            })
            .bytes,
            b"block",
        );
        assert_eq!(
            round_trip(&FetchBlob {
                ns: ns.clone(),
                id,
                range: Some(1..4)
            })
            .range,
            Some(1..4),
        );
        assert_eq!(round_trip(&HasBlob { ns: ns.clone(), id }).id, id);
        assert_eq!(
            round_trip(&DeleteNamespace {
                ns: ns.clone(),
                deleted_at: 7
            })
            .deleted_at,
            7
        );
        assert_eq!(round_trip(&StoreAck::Stored), StoreAck::Stored);
        assert_eq!(round_trip(&StoreAck::Deleted), StoreAck::Deleted);
        assert_eq!(round_trip(&DeleteAck::Acked), DeleteAck::Acked);
        // The anti-entropy pull and its snapshot reply cross node boundaries too.
        let _: SyncTombstones = round_trip(&SyncTombstones);
        let snapshot = vec![Tombstone { ns, deleted_at: 9 }];
        assert_eq!(round_trip(&snapshot), snapshot);
    }

    type Sim = actor_core::LocalSystem<
        actor_simulation::SimClock,
        actor_simulation::SimEntropy,
        actor_simulation::SimSpawner,
    >;

    #[test]
    fn the_replica_actor_serves_store_fetch_has_and_delete() {
        let sim = Simulation::new(1);
        let system: Sim =
            LocalSystemBuilder::new(sim.clock(), sim.entropy(), sim.spawner()).build();
        let dir = tempfile::tempdir().expect("tempdir");
        let local = LocalBlobStore::open(dir.path()).expect("open");

        sim.block_on(async move {
            let replica = system.spawn(BlobReplica::<Sim>::new(local, TombstoneSet::new()));
            system
                .receptionist()
                .register(blob_replica_key::<Sim>(), &replica);
            let within = Duration::from_secs(5);

            let ns = Namespace::new(b"workspace".to_vec());
            let bytes = b"a stored block".to_vec();
            let id = BlobId::of(&bytes);

            // Store, then read it back raw and probe presence.
            assert_eq!(
                replica
                    .ask_timeout(
                        StoreBlob {
                            ns: ns.clone(),
                            id,
                            bytes: bytes.clone()
                        },
                        within
                    )
                    .await,
                Ok(StoreAck::Stored),
            );
            assert_eq!(
                replica
                    .ask_timeout(
                        FetchBlob {
                            ns: ns.clone(),
                            id,
                            range: None
                        },
                        within
                    )
                    .await,
                Ok(Some(bytes)),
            );
            assert_eq!(
                replica
                    .ask_timeout(HasBlob { ns: ns.clone(), id }, within)
                    .await,
                Ok(true),
            );

            // Tombstone the namespace; the blob then resolves nowhere and a store
            // back into it is refused (spec §5.3, B7).
            assert_eq!(
                replica
                    .ask_timeout(
                        DeleteNamespace {
                            ns: ns.clone(),
                            deleted_at: 1
                        },
                        within
                    )
                    .await,
                Ok(DeleteAck::Acked),
            );
            assert_eq!(
                replica
                    .ask_timeout(
                        FetchBlob {
                            ns: ns.clone(),
                            id,
                            range: None
                        },
                        within
                    )
                    .await,
                Ok(None),
            );
            assert_eq!(
                replica
                    .ask_timeout(HasBlob { ns: ns.clone(), id }, within)
                    .await,
                Ok(false),
            );
            assert_eq!(
                replica
                    .ask_timeout(
                        StoreBlob {
                            ns,
                            id,
                            bytes: b"a stored block".to_vec()
                        },
                        within
                    )
                    .await,
                Ok(StoreAck::Deleted),
            );
        });
    }

    #[test]
    fn the_actor_transport_resolves_and_reaches_a_replica() {
        let sim = Simulation::new(2);
        let system: Sim =
            LocalSystemBuilder::new(sim.clock(), sim.entropy(), sim.spawner()).build();
        let dir = tempfile::tempdir().expect("tempdir");
        let local = LocalBlobStore::open(dir.path()).expect("open");

        sim.block_on(async move {
            let replica = system.spawn(BlobReplica::<Sim>::new(local, TombstoneSet::new()));
            system
                .receptionist()
                .register(blob_replica_key::<Sim>(), &replica);

            let node = system.node();
            let transport = ActorBlobTransport::new(system.clone());
            let within = Duration::from_secs(5);

            let ns = Namespace::new(b"ws".to_vec());
            let bytes = b"via the transport seam".to_vec();
            let id = BlobId::of(&bytes);

            assert_eq!(
                transport
                    .store_blob(node, ns.clone(), id, bytes.clone(), within)
                    .await,
                Ok(StoreAck::Stored),
            );
            assert_eq!(
                transport
                    .fetch_blob(node, ns.clone(), id, None, within)
                    .await,
                Ok(Some(bytes)),
            );
            assert_eq!(
                transport.has_blob(node, ns.clone(), id, within).await,
                Ok(true)
            );

            // A node with no registered replica is Unreachable, not a panic.
            let absent = NodeId::new(999);
            assert_eq!(
                transport.has_blob(absent, ns, id, within).await,
                Err(CallError::Unreachable),
            );
        });
    }
}
