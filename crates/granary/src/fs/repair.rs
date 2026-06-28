//! Grain-driven blob repair (durable-workspace design, granary §7.10 / §16 B6).
//!
//! A grain's *records* self-heal — every activation's quorum read-repair writes the
//! recovered tail back to the current quorum (G14) — but its *blobs* do not: lazy
//! fault backfills only the leader, and only for blocks actually read (§7.10). So a
//! block written while a replica was unreachable, or before a replica joined the
//! shard, can sit below the write quorum until that block happens to be read, and a
//! cold block never read could be lost under sustained churn.
//!
//! Repair closes that gap at the **consumer** level, the near-term stand-in for the
//! deferred framework-level §7.6 new-replica catch-up. Because the workspace grain
//! knows its own **live root set** (the block ids its [`FsTree`](super::meta::FsTree)
//! still references), repair is root-driven — it touches only referenced blocks, not
//! a liveness-blind probe of everything. It re-`put`s each live block (sourced via a
//! verifying `get`), which re-fans the bytes to the grain's *current* replicas:
//! replicas already holding it dedup (a no-op, B2), a replica that lacked it receives
//! it, restoring the R margin. Additive and idempotent; a failure is retried next pass.

use std::collections::BTreeSet;

use crate::BlobId;
use crate::GrainBlobs;

/// Re-replicate `live` to the grain's current shard replicas. Best-effort: kicked as
/// a background task off the activation latency path (see the grain's `on_activate`),
/// so it never blocks a command.
///
/// Cost note (§7.10 open question): a boolean `has` cannot prove a full R margin, so
/// v1 re-puts every live block rather than only the under-replicated ones. It is
/// idempotent, and `put` already drains to all reachable replicas at write time, so
/// the steady-state effect is a re-fan that holders dedup; the real work happens only
/// after a membership change. A `replicas_holding(id) -> usize` primitive or a
/// shard-reconfiguration signal would let repair skip fully-replicated blocks — folded
/// into §7.6 when that lands.
pub(crate) async fn repair(blobs: GrainBlobs, live: BTreeSet<BlobId>) {
    for id in live {
        // Source a verifying copy (leader-local once cached) and re-fan it to the
        // current replicas. A block no reachable replica holds cannot be sourced —
        // that is data loss the repair cannot undo, and it is simply skipped.
        if let Ok(bytes) = blobs.get(id, None).await {
            let _ = blobs.put(bytes).await;
        }
    }
}
