//! The per-node `BlobReplica` actor and its messages (spec §6).
//!
//! The `Clustered` tier reuses the actor framework's transport, with no new wire
//! protocol (actor §7.1). A per-node [`BlobReplica`] actor owns this node's
//! on-disk [`LocalBlobStore`] and accepts five messages: [`StoreBlob`],
//! [`FetchBlob`], [`HasBlob`], [`DeleteNamespace`], and [`SyncTombstones`]. It is
//! registered in the receptionist under one well-known key ([`blob_replica_key`])
//! so peers discover it, and the tier reaches a peer's replica by an ordinary
//! `ask`.
//!
//! Unlike entity's `StoreRecord`, [`StoreBlob`] carries **no shard, no `after`,
//! no term, and no `repair` flag** (spec §6): nothing needs fencing and nothing
//! needs ordering, so the only field beyond the bytes is the namespace the blob
//! lives under.

use std::marker::PhantomData;
use std::ops::Range;
use actor_core::Actor;
use actor_core::Ctx;
use actor_core::Handler;
use actor_core::HandlerRegistry;
use actor_core::Manifest;
use actor_core::Message;
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
/// well-known key, one entry per node (spec §6). The tier looks a peer's replica
/// up here, then `ask`s it.
pub fn blob_replica_key<S: BlobSystem>() -> Key<BlobReplica<S>> {
    Key::new("blob.replica")
}

/// Store one blob's bytes on a replica under `ns` (spec §6). The reply is a
/// [`StoreAck`].
#[derive(Serialize, Deserialize)]
pub struct StoreBlob {
    pub ns: Namespace,
    pub id: BlobId,
    /// Told to the codec as bytes rather than left to serde's default sequence
    /// path: worth ~160x on encode and ~670x on decode at a mebibyte.
    #[serde(with = "serde_bytes")]
    pub bytes: Vec<u8>,
}

impl Message for StoreBlob {
    type Reply = StoreAck;
    const MANIFEST: Manifest = Manifest::new("blob.StoreBlob");
}

/// Fetch one blob's **whole** raw bytes from a replica (spec §6). The reply is
/// `Some(bytes)` if the replica holds it, else `None` (absent or the namespace is
/// tombstoned). The bytes are **not** verified by the replica: the caller
/// re-hashes them after transfer (spec §4, §5.2, **B1**). `range` is reserved for
/// range-verified streaming (spec §10); v1 returns the whole blob.
#[derive(Serialize, Deserialize)]
pub struct FetchBlob {
    pub ns: Namespace,
    pub id: BlobId,
    pub range: Option<Range<u64>>,
}

impl Message for FetchBlob {
    /// `ByteBuf` for the same reason [`StoreBlob::bytes`] is tagged: this is the read
    /// half of the same payload. The tier unwraps it to `Vec<u8>`, a move, not a
    /// copy.
    type Reply = Option<serde_bytes::ByteBuf>;
    const MANIFEST: Manifest = Manifest::new("blob.FetchBlob");
}

/// Whether a replica holds `(ns, id)` (spec §6): the per-owner probe that gates
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
/// delete is harmless. The reply is a [`DeleteAck`]; `deleted_at` is the anchoring
/// stamp (see [`Tombstone`]).
#[derive(Serialize, Deserialize)]
pub struct DeleteNamespace {
    pub ns: Namespace,
    pub deleted_at: u64,
}

impl Message for DeleteNamespace {
    type Reply = DeleteAck;
    const MANIFEST: Manifest = Manifest::new("blob.DeleteNamespace");
}

/// Ask a replica for its whole tombstone awareness set (spec §5.3): the
/// **anti-entropy pull** the reconcile loop runs each pass so a node that was
/// partitioned or down across a `delete_namespace` learns the tombstones it
/// missed (**B7**). The pull runs after the pass's re-replication push, so what
/// stops a stale copy from being re-pushed is the receiving owner's
/// `StoreAck::Deleted`; without the pull, a healed holder never sweeps its own
/// copy and keeps serving a namespace the fan-out never reached it with.
#[derive(Serialize, Deserialize)]
pub struct SyncTombstones;

impl Message for SyncTombstones {
    type Reply = Vec<Tombstone>;
    const MANIFEST: Manifest = Manifest::new("blob.SyncTombstones");
}

/// A replica's response to [`StoreBlob`] (spec §6). It has **no** `Fenced` or
/// `Stale` variant (contrast entity's `StoreAck`): there is no term and no
/// mutable head to be stale against.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum StoreAck {
    /// The blob is durably stored on this replica (or was already present, B2).
    Stored,
    /// The target namespace is tombstoned, so the store was refused (spec §5.3).
    Deleted,
}

/// A replica's response to [`DeleteNamespace`] (spec §6): the tombstone is durably
/// recorded.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum DeleteAck {
    /// The tombstone is durably recorded on this replica; its bytes are swept.
    Acked,
}

/// The node-local replica store (spec §6): a thin actor over this node's
/// [`LocalBlobStore`], reachable across the cluster. One per node, registered
/// under [`blob_replica_key`].
///
/// A `DeleteNamespace` also updates this node's in-memory [`TombstoneSet`], so the
/// on-disk tombstone (which refuses stores and survives restart) and the in-memory
/// set (which the tier's `get`/`put` consult and gossip) move together (spec §5.3).
pub struct BlobReplica<S: BlobSystem> {
    store: LocalBlobStore,
    tombstones: TombstoneSet,
    _marker: PhantomData<fn() -> S>,
}

impl<S: BlobSystem> BlobReplica<S> {
    /// Wrap this node's on-disk store as a replica actor, updating `tombstones`
    /// alongside it.
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
            // A non-tombstone store failure means this node's disk is broken, and a
            // replica cannot function without durable storage. The panic stops the
            // actor and prunes its registration, so the put falls back to another
            // owner and reconcile re-replicates the blob to a healthy one (B6).
            Err(err) => panic!("blob replica failed to store {}: {err}", msg.id),
        }
    }
}

impl<S: BlobSystem> Handler<FetchBlob> for BlobReplica<S> {
    async fn handle(
        &mut self,
        msg: FetchBlob,
        _ctx: &Ctx<BlobReplica<S>>,
    ) -> Option<serde_bytes::ByteBuf> {
        // Raw, unverified bytes: the caller verifies after transfer (B1, §5.2).
        self.store
            .read_raw(&msg.ns, &msg.id)
            .map(serde_bytes::ByteBuf::from)
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
        // then in-memory awareness. Both are monotonic (spec §5.3, §6).
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
        // Cheap: one tiny record per deleted namespace.
        self.tombstones.snapshot()
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use super::*;
    use actor_core::ActorSystem;
    use actor_core::LocalSystemBuilder;
    use actor_simulation::Simulation;

    fn round_trip<T>(value: &T) -> T
    where
        T: Serialize + serde::de::DeserializeOwned,
    {
        // The replica messages cross node boundaries, so each must survive a codec
        // round-trip (V&V checklist #1); serde_json stands in for the wire codec.
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
                Ok(Some(serde_bytes::ByteBuf::from(bytes))),
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
}
