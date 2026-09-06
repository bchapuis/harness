//! The drive's observability events (drive spec §8): checker-facing, ephemeral,
//! describing the *machinery around* effects and carrying nothing about their
//! content — the payload is the consumer's and belongs in the consumer's records
//! (harness §10.1).
//!
//! Each event rides the core stream as [`Event::App`] (core §16), alongside the
//! grain's own (granary §13), so a checker observes one totally ordered sequence
//! and the seed-reproducibility contract covers these too. Recover them with
//! `event.as_app::<DriveEvent>()`.
//!
//! The vocabulary is kept to what a stream checker cannot reconstruct from the
//! grain's own events: activation, deactivation, leadership, and commit are read
//! from `Activated`, `Deactivated`, `LeaderChanged`, and `Committed`, and the
//! drive mints no duplicates of them. [`Discarded`](DriveEvent::Discarded) is
//! present because a discarded outcome leaves no record anywhere — the one thing
//! the drive does that the journal cannot show, and D5 would be uncheckable
//! without it.

use actor_core::Event;
use actor_core::NodeId;
use granary::GrainName;

/// One drive event (drive spec §8).
///
/// `key` is the effect's key rendered with its `Debug` form: the checkers
/// compare keys for identity, and a `DriveEvent` must be one concrete type
/// across every driven grain type on the stream, so it cannot be generic over
/// `Driven::Key`.
///
/// Every variant carries the `node` hosting the activation, as the grain's own
/// events do (granary §13) and for the same reason: a claim's life is
/// per-activation, and under failover two activations of one grain — a deposed
/// leader still running down its launches and its successor — are distinguishable
/// on the stream only by the node that emits.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum DriveEvent {
    /// An effect was claimed and handed to the spawner (§3.2). Exactly one per
    /// claim: a re-drive never relaunches an effect in flight (D3).
    Launched {
        grain: GrainName,
        key: String,
        node: NodeId,
    },
    /// A dangling member declaring `Interrupt` was resolved by the activation's
    /// first drive without a launch (§4.4): its synthesized `Interrupted` outcome
    /// is on its way through the mailbox. Opens the claim its `Settled` closes,
    /// so D6's "none dropped, none resolved twice" reads off the stream.
    Interrupted {
        grain: GrainName,
        key: String,
        node: NodeId,
    },
    /// An outcome reached `settle`.
    ///
    /// Fires on **every** settle that runs, not only on one that writes. A
    /// settle may legitimately return no records (§4.2), and that is exactly the
    /// path whose key stays in the desired set to be relaunched under backoff —
    /// so an event that fired only on a commit would leave a `Launched` with no
    /// terminator on the stream, and D3's and D7's windows could not be closed.
    /// `committed: false` is also the only signal a checker gets that an outcome
    /// arrived and changed nothing.
    ///
    /// Emitted **after** the records commit when there are records to commit
    /// (§8): the drive stages it and releases it from the drive that follows the
    /// commit, so a settle whose append fails — the activation stepping down —
    /// leaves no `Settled` claiming records that never became durable. A settle
    /// that decided no records has nothing to wait for and emits at once.
    Settled {
        grain: GrainName,
        key: String,
        node: NodeId,
        /// `Ok` versus `Err` (§4.3), and nothing more about either.
        ok: bool,
        /// Whether the settle's records committed.
        committed: bool,
    },
    /// A straggling outcome was dropped: its key had left the desired set by the
    /// time it arrived (§4.5). No record is written for it.
    Discarded {
        grain: GrainName,
        key: String,
        node: NodeId,
    },
    /// The desired set came to rest and nothing is in flight (§6), so the
    /// activation now permits hibernation.
    Quiescent { grain: GrainName, node: NodeId },
}

impl From<DriveEvent> for Event {
    fn from(event: DriveEvent) -> Event {
        Event::app(event)
    }
}
