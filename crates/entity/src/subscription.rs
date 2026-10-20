//! Record subscriptions: the journal follower (spec §7.3).
//!
//! A subscription is a live, best-effort push of an entity's committed records to
//! a sink, layered over the durable `load` (storage §3.1): off the write path, bounded,
//! and never authoritative. The host delivers each committed batch at the same
//! point it emits `Committed` (§11), after the fold and the output gate (§6
//! step 4), so delivery cannot affect a write's outcome (**E5**).
//!
//! The contract is **reconcile by `Seq`** (§7.3, **E16**): a batch carries the
//! `from` it begins after; a subscriber closes any gap with `load` and ignores
//! anything at or below what it has seen. At-most-once delivery (actor §8.1) may
//! drop, duplicate, or (across a re-subscribe) reorder; seq reconciliation
//! absorbs all three, so correctness rests on the journal, not on delivery.
//! [`Follower`] is that reconciliation, so a caller that wants the committed
//! sequence pulls it from [`EntityRef::follow`] instead of writing its own.
//!
//! The wire batch ([`RecordBatch`]) carries opaque **tagged** record bytes:
//! exactly what the host appended (facets §4), so the always-on subscribe path
//! imposes no `Clone` bound on an entity's `Event`. The sink ([`RecordSink`]) is
//! the entity's **event projection**: it decodes facet-0 records to the typed
//! `Event` and skips other facets' records, whose seqs appear as gaps the
//! subscriber's seq-reconciliation already absorbs. The wire types stay inside
//! the crate, so the facet envelope never reaches a caller.

use std::time::Duration;

use actor_core::Actor;
use actor_core::ActorRef;
use actor_core::ActorSystem;
use actor_core::Ctx;
use actor_core::Handler;
use actor_core::HandlerRegistry;
use actor_core::Manifest;
use actor_core::Message;
use futures::future::Either;
use futures::future::select;
use serde::Deserialize;
use serde::Serialize;

use crate::entity::Entity;
use crate::entity_ref::EntityRef;
use crate::error::EntityError;
use crate::journal::Seq;

/// Delivery buffer depth, on both legs (host→forwarder and sink→subscriber).
/// On overflow the producing side drops rather than blocks (§7.3): a slow
/// subscriber loses its place and backfills by `load`, never stalling a write.
pub(crate) const SUB_BUFFER: usize = 128;

/// Journal page size for a follower's backfill reads.
const FOLLOW_PAGE: usize = 256;

/// How long a caught-up follower waits for a live record before re-checking the
/// journal. A silent leader move or crash leaves the old sink alive but idle
/// (the stream never closes and the new leader has no sink), so a periodic
/// backfill is the liveness net that detects the move and re-subscribes (§7.3).
const FOLLOW_RESYNC: Duration = Duration::from_secs(2);

/// One pushed batch on the wire (spec §7.3): the seqs just committed, as opaque
/// tagged record bytes, beginning after `from`. The bytes are the records as
/// the journal stores them (facets §4); [`RecordSink`] decodes the event-tagged
/// ones and skips the rest.
#[derive(Clone, Serialize, Deserialize)]
pub(crate) struct RecordBatch {
    /// The exclusive lower bound: these records are the slots after `from`.
    pub(crate) from: Seq,
    /// `(seq, tagged record bytes)` for each committed record in the batch.
    pub(crate) records: Vec<(Seq, Vec<u8>)>,
}

impl Message for RecordBatch {
    type Reply = ();
    const MANIFEST: Manifest = Manifest::new("entity.RecordBatch");
}

/// The reply to [`Subscribe`]: the entity's committed head at registration time,
/// so the subscriber knows how far to backfill (`from`..`head`) before live
/// batches take over (§7.3).
#[derive(Clone, Serialize, Deserialize)]
pub(crate) struct Subscribed {
    pub(crate) head: Seq,
}

/// Register a record sink with an entity (spec §7.3). A framework built-in
/// dispatched to the host for *any* entity type (it never appears in the entity's
/// `register` allowlist, §5.5): the push analogue of `head`/`load`.
#[derive(Serialize, Deserialize)]
#[serde(bound = "")]
pub(crate) struct Subscribe<G: Entity> {
    /// The exclusive lower bound the subscriber wants live records after. The
    /// host does not consult it: the subscriber backfills from the head in
    /// [`Subscribed`] plus `load`.
    pub(crate) from: Seq,
    /// Where committed batches are pushed.
    pub(crate) sink: ActorRef<RecordSink<G>>,
}

impl<G: Entity> Message for Subscribe<G> {
    type Reply = Subscribed;
    const MANIFEST: Manifest = Manifest::new("entity.Subscribe");
}

/// Stop a [`RecordSink`] (spec §7.3), closing the subscriber's stream. Two
/// senders: the `subscribe` error path, for a sink spawned but never registered
/// (so the actor does not linger until system shutdown), and the host's
/// forwarder, once the host has dropped the sink (a lag-drop, hibernation, a
/// move, or a forced step-down), so the subscriber observes its stream close
/// and re-subscribes rather than waiting on a push that will never come. The
/// host may lead the entity's shard from another node, so this crosses the wire.
#[derive(Serialize, Deserialize)]
pub(crate) struct CloseSink;

impl Message for CloseSink {
    type Reply = ();
    const MANIFEST: Manifest = Manifest::new("entity.CloseSink");
}

/// A decoded batch handed to the subscriber over the subscription channel: the
/// typed events and the `from` they begin after (§7.3).
pub struct RecordStream<E> {
    /// The exclusive lower bound: a `from` above the subscriber's last seq is a
    /// gap it closes by reading the journal.
    pub from: Seq,
    /// The batch's events at their journal slots; facet records leave gaps.
    pub records: Vec<(Seq, E)>,
}

/// What [`EntityRef::subscribe`] returns: the head at registration, and the
/// live record stream. The subscriber backfills `from`..`head` by `load`
/// (storage §3.1), then reconciles live batches by `Seq` (§7.3, **E16**); when the
/// stream closes (a move, a lag-drop, hibernation) it re-subscribes and
/// backfills from its last seq.
pub struct Subscription<G: Entity> {
    pub head: Seq,
    pub records: async_channel::Receiver<RecordStream<G::Event>>,
}

/// The sink actor a subscriber spawns and hands to the host (spec §7.3). It
/// receives wire [`RecordBatch`]es (possibly from another node), decodes them to
/// the entity's typed `Event`, and forwards them on a bounded channel to the
/// subscriber. A concrete actor type, since `ActorRef` is typed (no erased
/// recipient).
pub(crate) struct RecordSink<G: Entity> {
    tx: async_channel::Sender<RecordStream<G::Event>>,
}

impl<G: Entity> RecordSink<G> {
    pub(crate) fn new(tx: async_channel::Sender<RecordStream<G::Event>>) -> RecordSink<G> {
        RecordSink { tx }
    }
}

impl<G: Entity> Actor for RecordSink<G> {
    type System = G::System;

    /// Accept pushed batches, and the close, over the network: the host that
    /// leads the entity's shard may be another node (§5.2), so these are the
    /// deserialization allowlist entries for `RecordBatch` and `CloseSink`
    /// (actor §3.4, §14).
    fn register(registry: &mut HandlerRegistry<Self>) {
        registry.accept::<RecordBatch>();
        registry.accept::<CloseSink>();
    }
}

impl<G: Entity> Handler<RecordBatch> for RecordSink<G> {
    async fn handle(&mut self, msg: RecordBatch, ctx: &Ctx<Self>) {
        let codec = ctx.system().codec();
        let mut records = Vec::with_capacity(msg.records.len());
        for (seq, bytes) in &msg.records {
            // Every record wears its facet tag (spec facets §4); the event
            // projection keeps facet 0 and skips the rest (see module docs).
            let Ok((tag, payload)) = crate::facet::split_record(bytes) else {
                return;
            };
            if tag != crate::facet::EVENT_TAG {
                continue;
            }
            match actor_serialization::decode::<G::Event>(&*codec, payload) {
                Ok(event) => records.push((*seq, event)),
                // A record that will not decode: drop the whole batch. The
                // subscriber sees a gap and backfills by `load` (§7.3): the
                // journal is the authority (**E3**), not this delivery.
                Err(_) => return,
            }
        }
        // try_send, not send: a slow subscriber loses batches on overflow rather
        // than back-pressuring the host's forwarder (§7.3). A `Full` overflow
        // keeps the sink alive; the subscriber reconciles by seq. A `Closed`
        // channel will never be read again, so the sink stops rather than
        // lingering until system shutdown (the framework does not reap actors on
        // ref drop).
        if let Err(async_channel::TrySendError::Closed(_)) = self.tx.try_send(RecordStream {
            from: msg.from,
            records,
        }) {
            ctx.stop();
        }
    }
}

impl<G: Entity> Handler<CloseSink> for RecordSink<G> {
    async fn handle(&mut self, _msg: CloseSink, ctx: &Ctx<RecordSink<G>>) {
        ctx.stop();
    }
}

/// What one [`Follower::next`] yields: the next events, or the one thing that
/// breaks the exact-sequence contract, snapshot compaction truncating the
/// journal past the follower's cursor (storage §9).
#[derive(Debug, Clone, PartialEq)]
pub enum Followed<E> {
    /// The next in-order events after the cursor, at least one, at their
    /// journal slots (facet records occupy the ones skipped, facets §4).
    Batch(Vec<(Seq, E)>),
    /// The journal compacted past the cursor: the records in `(cursor, base]`
    /// are subsumed by the entity's snapshot and can no longer be read, by
    /// this follower or anyone. The stream resumes after `base`; the signal is
    /// surfaced (rather than silently jumped) so an observer never mistakes
    /// truncated history for a complete one.
    Truncated { base: Seq },
}

/// A live follower over an entity's committed events (§7.3), from
/// [`EntityRef::follow`]. It rides a record subscription and reconciles by
/// `Seq`: it backfills from the journal on first attach, on any gap, and after
/// the stream closes (a leader move, a lag-drop, or hibernation), so
/// [`next`](Self::next) yields the exact committed event sequence, in order,
/// with no gap or duplicate (**E16**); except where compaction has truncated
/// it, which it reports as [`Followed::Truncated`] rather than papering over.
/// Push is the fast path; the journal is the authority.
pub struct Follower<G: Entity> {
    entity: EntityRef<G>,
    /// For the re-sync liveness timer (a silent move leaves the stream open).
    system: G::System,
    /// The live subscription, or `None` before the first attach / after a close.
    sub: Option<Subscription<G>>,
    /// The highest seq handed to the caller: the reconciliation cursor.
    last: Seq,
}

impl<G: Entity> Follower<G> {
    pub(crate) fn new(entity: EntityRef<G>, system: G::System, from: Seq) -> Follower<G> {
        Follower {
            entity,
            system,
            sub: None,
            last: from,
        }
    }

    /// The next in-order events after the last ones returned (at least one),
    /// or a [`Followed::Truncated`] when compaction has outrun the cursor.
    /// Blocks until events are available, attaching, backfilling, and
    /// re-subscribing transparently. An `EntityError` is a real durability
    /// outcome (the shard cannot serve right now) the caller may surface and
    /// retry.
    pub async fn next(&mut self) -> Result<Followed<G::Event>, EntityError> {
        loop {
            // (Re)attach if needed: subscribe registers a sink and returns the
            // head, so any commit from here on is pushed; the backfill below
            // closes whatever gap preceded the attach (a late start or a move).
            if self.sub.is_none() {
                self.sub = Some(self.entity.subscribe(self.last).await?);
            }
            // Backfill straight from the journal (the source of truth) until the
            // cursor reaches the head; return as soon as a page has events.
            if let Some(step) = self.backfill().await? {
                return Ok(step);
            }
            // Caught up: race the next live batch against a re-sync timer. Clone
            // the receiver so no borrow of `self.sub` is held across the await.
            let rx = self.sub.as_ref().expect("attached").records.clone();
            let recv = rx.recv();
            let resync = self.system.sleep(FOLLOW_RESYNC);
            futures::pin_mut!(recv);
            match select(recv, resync).await {
                // A live batch: reconcile by seq.
                Either::Left((Ok(stream), _)) => {
                    if stream.from <= self.last {
                        let fresh: Vec<(Seq, G::Event)> = stream
                            .records
                            .into_iter()
                            .filter(|(seq, _)| *seq > self.last)
                            .collect();
                        if let Some((seq, _)) = fresh.last() {
                            self.last = *seq;
                            return Ok(Followed::Batch(fresh));
                        }
                    }
                    // A gap (`from > last`, a lag-drop) or all duplicates (a
                    // re-subscribe replay): fall through; the loop re-backfills.
                }
                // The stream closed (a clean step-down dropped the sink): re-attach.
                Either::Left((Err(_), _)) => self.sub = None,
                // The timer won: a silent move/crash leaves the stream open but
                // dead. If the head advanced anyway, re-subscribe to the current
                // leader (the old sink is orphaned) and return the backfill; else
                // we are simply idle; keep the subscription, no churn.
                Either::Right(_) => {
                    if let Some(step) = self.backfill().await? {
                        self.sub = None;
                        return Ok(step);
                    }
                }
            }
        }
    }

    /// One page of events after the cursor, read from the journal (the
    /// non-activating gateway read, §7), advancing the cursor. `None` when
    /// already at the head. A compaction base above the cursor is the
    /// truncation signal: the cursor jumps to the base and the fetched events
    /// are dropped; the next call re-reads from the base, so the one-page
    /// waste buys a caller that always hears about the gap *before* any event
    /// past it.
    async fn backfill(&mut self) -> Result<Option<Followed<G::Event>>, EntityError> {
        let history = self.entity.events(self.last, FOLLOW_PAGE).await?;
        if history.base > self.last {
            self.last = history.base;
            return Ok(Some(Followed::Truncated { base: history.base }));
        }
        match history.events.last() {
            Some((seq, _)) => {
                self.last = *seq;
                Ok(Some(Followed::Batch(history.events)))
            }
            None => Ok(None),
        }
    }
}
