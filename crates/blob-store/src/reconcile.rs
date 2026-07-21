//! The push-based, tombstone-respecting rebalance loop (spec §7, **B6**) and the
//! tombstone reclamation pass (spec §5.3, **B7** liveness).
//!
//! Owner selection is a pure function of the membership view (spec §5.2), so as
//! nodes join and leave, the *intended* placement of every blob changes
//! automatically and minimally (utilities U1). Recomputing owners does not move
//! bytes, though — restoring the durability target after a change requires this
//! active loop, modeled on granary's `shardmap.rs` reconcile but copying blob
//! bytes rather than reconfiguring a Raft group.
//!
//! The loop is **decentralized**: each node, periodically, ensures every blob it
//! holds is present on that blob's *current* top-`R` owners, `StoreBlob`-ing to any
//! owner that lacks it (a `HasBlob` gates the copy). It is **additive** — it only
//! restores copies, never drops them; bytes are removed on exactly one path,
//! `delete_namespace` (spec §5.3), never by an inference here, which cannot know
//! whether a misplaced copy is still wanted. A blob whose namespace is tombstoned
//! is **never copied**: the loop skips it, and the receiving owner would reject it
//! anyway, so rebalancing cannot resurrect a deleted namespace (**B7** safety).
//!
//! The same pass reclaims tombstones whose retention is no longer needed: once
//! every member at anchor time has swept or reached a terminal state, the anchor's
//! sweep-tracking bookkeeping is released (spec §5.3).

use std::sync::Weak;
use std::time::Duration;

use crate::cluster::Inner;
use crate::event::BlobEvent;
use crate::placement;
use crate::system::BlobSystem;

/// The period between reconcile passes. Frequent enough to restore the `R` margin
/// promptly after a departure, coarse enough that steady-state probing is cheap.
/// The loop runs on the framework's `Clock`/`Spawner`, so it is seed-reproducible
/// under simulation (spec §8).
const RECONCILE_INTERVAL: Duration = Duration::from_millis(250);

/// How long a reconcile copy or probe waits on an owner before giving up; the blob
/// is retried on the next pass, so a missed copy is eventually consistent.
const RECONCILE_TIMEOUT: Duration = Duration::from_secs(2);

/// How long a tombstone anti-entropy pull waits on a peer. Much shorter than
/// [`RECONCILE_TIMEOUT`]: the pull is best-effort and retried every pass, so a peer
/// that does not answer promptly is simply skipped this round rather than stalling
/// the pass — which keeps B6 re-replication snappy when a peer is unreachable.
const SYNC_TIMEOUT: Duration = Duration::from_millis(500);

/// The reconcile loop for one node. Holds a `Weak` to the node's [`Inner`] and
/// exits once the last [`ClusteredBlobStore`](crate::ClusteredBlobStore) handle is
/// dropped, so it never keeps a torn-down node alive (the granary pattern).
pub(crate) async fn reconcile_loop<S: BlobSystem>(inner: Weak<Inner<S>>) {
    loop {
        let Some(inner) = inner.upgrade() else {
            return; // the store was dropped — stop reconciling.
        };
        reconcile_pass(&inner).await;
        // Drop the strong reference before sleeping so the store can be dropped
        // during the idle interval (the loop exits next wake-up).
        let system = inner.system.clone();
        drop(inner);
        system.sleep(RECONCILE_INTERVAL).await;
    }
}

/// One reconcile pass: restore under-replicated blobs (B6), then reclaim
/// tombstones whose retention is no longer required (B7 liveness).
async fn reconcile_pass<S: BlobSystem>(inner: &Inner<S>) {
    let members = inner.system.serving_members();
    let r = inner.config.replication_factor;

    // --- Re-replication: push every local blob to its current owners (B6) -------
    for (ns, id) in inner.local.blobs() {
        // Never copy a blob of a tombstoned namespace — that is the move that could
        // resurrect a deleted namespace (spec §5.3, §7, B7 safety).
        if inner.tombstones.contains(&ns) {
            continue;
        }
        for owner in placement::owners(&members, &ns, &id, r) {
            if owner == inner.self_node {
                continue; // this node already holds it.
            }
            // Only copy to an owner that lacks it (the `HasBlob` gate). An owner we
            // cannot reach (`Err`) or that already holds it (`Ok(true)`) is left
            // alone — an unreachable owner is retried next pass, so rebalancing is
            // eventually consistent and never blocks.
            if let Ok(false) = inner
                .transport
                .has_blob(owner, ns.clone(), id, RECONCILE_TIMEOUT)
                .await
                && let Some(bytes) = inner.local.read_raw(&ns, &id)
            {
                let _ = inner
                    .transport
                    .store_blob(owner, ns.clone(), id, bytes, RECONCILE_TIMEOUT)
                    .await;
            }
        }
    }

    // --- Anti-entropy: learn tombstones this node missed (spec §5.3 gossip, B7) --
    // The initial `delete_namespace` fan-out reaches every *then-serving* node, but a
    // node partitioned or down across the delete misses it. Pull every serving peer's
    // awareness set and apply any tombstone we lack — durable record + sweep, then
    // in-memory awareness — so a rejoining holder stops serving (and stops re-pushing)
    // a stale blob of a deleted namespace. Without it, a healed `unreachable` holder
    // resurrects the namespace it never learned was gone (B7). This runs *after*
    // re-replication, and the pulls run concurrently, so one slow or dead peer never
    // starves B6 re-replication; the push vector is independently guarded by the
    // receiving owner's `StoreAck::Deleted` reject while gossip converges.
    sync_tombstones(inner, &members).await;

    // --- Reclamation: release the anchor's sweep tracking when safe (B7) --------
    // Once every member at anchor time has swept or reached a terminal state, no
    // node can still hold an un-swept blob of the namespace, so the per-namespace
    // sweep bookkeeping is no longer needed (spec §5.3). The tiny awareness flag in
    // the tombstone set is retained (cluster-wide set GC is a future refinement).
    for ns in inner.anchors.tracked() {
        if inner
            .anchors
            .reclaimable(&ns, |node| inner.system.is_terminal(node))
        {
            inner.anchors.reclaim(&ns);
        }
    }
}

/// Pull every serving peer's tombstone set and apply the ones this node lacks
/// (spec §5.3, **B7**). A newly-learned tombstone is recorded durably and its
/// bytes swept — the same effect a `DeleteNamespace` would have — then added to the
/// in-memory awareness set so `put`/`get`/reconcile short-circuit, and finally
/// emitted so the simulator's checkers observe the node becoming aware. Peers are
/// visited in a stable (sorted) order, and only genuinely new tombstones mutate
/// state, so the pass is idempotent and seed-reproducible.
async fn sync_tombstones<S: BlobSystem>(inner: &Inner<S>, members: &[actor_core::NodeId]) {
    let mut peers: Vec<actor_core::NodeId> = members
        .iter()
        .copied()
        .filter(|n| *n != inner.self_node)
        .collect();
    peers.sort();

    // Pull every peer concurrently, so a single slow or dead peer (whose ask runs to
    // the timeout) does not serialize the pass behind it. The asks are read-only; the
    // learned tombstones are applied below in the deterministic peer order, so the
    // emitted event stream stays seed-reproducible (spec §8).
    let pulls = peers
        .iter()
        .map(|&peer| inner.transport.sync_tombstones(peer, SYNC_TIMEOUT));
    let snapshots = futures::future::join_all(pulls).await;

    for snapshot in snapshots {
        let Ok(tombstones) = snapshot else {
            continue; // an unreachable peer is retried next pass
        };
        for tombstone in tombstones {
            if inner.tombstones.contains(&tombstone.ns) {
                continue; // already known — do not re-sweep or re-emit
            }
            // Learn it exactly as a DeleteNamespace would: durable tombstone + sweep
            // first (refuses later stores, survives restart), then awareness. If the
            // durable write fails, leave it unlearned so the next pass retries.
            if inner
                .local
                .tombstone(&tombstone.ns, tombstone.deleted_at)
                .is_ok()
            {
                inner.tombstones.insert(&tombstone.ns, tombstone.deleted_at);
                inner.system.emit_blob_event(BlobEvent::Tombstoned {
                    node: inner.self_node,
                    ns: tombstone.ns,
                });
            }
        }
    }
}
