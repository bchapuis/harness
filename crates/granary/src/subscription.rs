//! Record subscriptions: the journal follower (spec §7.9).
//!
//! A subscription is a live, best-effort push of a grain's committed records to
//! a sink, layered over the durable `load` (§7.3). It is the push analogue of a
//! §7.5 read: off the write path, bounded, and never authoritative. The host
//! delivers each committed batch at the same point it emits `Committed` (§13),
//! after the fold and the output gate (§6 step 4), so delivery cannot affect a
//! write's outcome (**G5**).
//!
//! The contract is **reconcile by `Seq`** (§7.9, **G16**): a batch carries the
//! `from` it begins after; a subscriber closes any gap with `load` and ignores
//! anything at or below what it has seen. At-most-once delivery (actor §7.2) may
//! drop, duplicate, or — across a re-subscribe — reorder; seq reconciliation
//! absorbs all three, so correctness rests on the journal, not on delivery.
//!
//! This module is grain-type-agnostic: the wire batch ([`RecordBatch`]) carries
//! opaque, codec-encoded record bytes — exactly what the host already encoded
//! for the append (§7.3) — so the always-on subscribe path imposes no `Clone`
//! bound on a grain's `Event`. The sink ([`RecordSink`]) decodes them to the
//! grain's typed `Event` for its subscriber.

use actor_core::Actor;
use actor_core::ActorRef;
use actor_core::ActorSystem;
use actor_core::Ctx;
use actor_core::Handler;
use actor_core::HandlerRegistry;
use actor_core::Manifest;
use actor_core::Message;
use serde::Deserialize;
use serde::Serialize;

use crate::grain::Grain;
use crate::journal::Seq;

/// Delivery buffer depth, on both legs (host→forwarder and sink→subscriber).
/// On overflow the producing side drops rather than blocks (§7.9): a slow
/// subscriber loses its place and backfills by `load`, never stalling a write.
pub(crate) const SUB_BUFFER: usize = 128;

/// One pushed batch on the wire (spec §7.9): the seqs just committed, as opaque
/// codec-encoded record bytes, beginning after `from`. The bytes are the grain's
/// `Event`s as the journal stores them (§7.3); [`RecordSink`] decodes them.
#[derive(Clone, Serialize, Deserialize)]
pub struct RecordBatch {
    /// The exclusive lower bound: these records are the slots after `from`.
    pub from: u64,
    /// `(seq, encoded event)` for each committed record in the batch.
    pub records: Vec<(u64, Vec<u8>)>,
}

impl Message for RecordBatch {
    type Reply = ();
    const MANIFEST: Manifest = Manifest::new("granary.RecordBatch");
}

/// The reply to [`Subscribe`]: the grain's committed head at registration time,
/// so the subscriber knows how far to backfill (`from`..`head`) before live
/// batches take over (§7.9).
#[derive(Clone, Serialize, Deserialize)]
pub struct Subscribed {
    pub head: Seq,
}

/// Register a record sink with a grain (spec §7.9). A framework built-in
/// dispatched to the host for *any* grain type (it never appears in the grain's
/// `register` allowlist, §5.5) — the push analogue of `head`/`load`.
#[derive(Serialize, Deserialize)]
#[serde(bound = "")]
pub struct Subscribe<G: Grain> {
    /// The exclusive lower bound the subscriber wants live records after.
    /// Carried for symmetry and future use; the head in [`Subscribed`] plus
    /// `load` is what the subscriber backfills from today.
    pub(crate) from: u64,
    /// Where committed batches are pushed.
    pub(crate) sink: ActorRef<RecordSink<G>>,
}

impl<G: Grain> Subscribe<G> {
    pub(crate) fn new(from: Seq, sink: ActorRef<RecordSink<G>>) -> Subscribe<G> {
        Subscribe {
            from: from.value(),
            sink,
        }
    }
}

impl<G: Grain> Message for Subscribe<G> {
    type Reply = Subscribed;
    const MANIFEST: Manifest = Manifest::new("granary.Subscribe");
}

/// Stop a [`RecordSink`] that was spawned but never registered (spec §7.9): the
/// `subscribe` error path tells it this so the actor does not linger until system
/// shutdown (a registered sink instead stops itself when its channel closes).
/// Local-only — sent by the spawning node, never over the wire.
#[derive(Serialize, Deserialize)]
pub(crate) struct CloseSink;

impl Message for CloseSink {
    type Reply = ();
    const MANIFEST: Manifest = Manifest::new("granary.CloseSink");
}

/// A decoded batch handed to the subscriber over the subscription channel: the
/// typed records and the `from` they begin after (§7.9).
pub struct RecordStream<E> {
    pub from: Seq,
    pub records: Vec<(Seq, E)>,
}

/// What [`GrainRef::subscribe`](crate::GrainRef::subscribe) returns: the head at
/// registration, and the live record stream. The subscriber backfills
/// `from`..`head` by `load` (§7.3), then reconciles live batches by `Seq`
/// (§7.9, **G16**); when the stream closes (a move, a lag-drop, hibernation) it
/// re-subscribes and backfills from its last seq.
pub struct Subscription<G: Grain> {
    pub head: Seq,
    pub records: async_channel::Receiver<RecordStream<G::Event>>,
}

/// The sink actor a subscriber spawns and hands to the host (spec §7.9). It
/// receives wire [`RecordBatch`]es (possibly from another node), decodes them to
/// the grain's typed `Event`, and forwards them on a bounded channel to the
/// subscriber. The concrete sink type the framework needs since `ActorRef` is
/// typed (no erased recipient) — the journal analogue of the harness's
/// `ReplyMailbox` (harness §7.4).
pub struct RecordSink<G: Grain> {
    tx: async_channel::Sender<RecordStream<G::Event>>,
}

impl<G: Grain> RecordSink<G> {
    pub(crate) fn new(tx: async_channel::Sender<RecordStream<G::Event>>) -> RecordSink<G> {
        RecordSink { tx }
    }
}

impl<G: Grain> Actor for RecordSink<G> {
    type System = G::System;

    /// Accept pushed batches over the network: the host that leads the grain's
    /// shard may be another node (§5.2), so this is the deserialization
    /// allowlist entry for `RecordBatch` (actor §5, §15).
    fn register(registry: &mut HandlerRegistry<Self>) {
        registry.accept::<RecordBatch>();
    }
}

impl<G: Grain> Handler<RecordBatch> for RecordSink<G> {
    async fn handle(&mut self, msg: RecordBatch, ctx: &Ctx<Self>) {
        let codec = ctx.system().codec();
        let mut records = Vec::with_capacity(msg.records.len());
        for (seq, bytes) in &msg.records {
            match actor_serialization::decode::<G::Event>(&*codec, bytes) {
                Ok(event) => records.push((Seq::new(*seq), event)),
                // A record that will not decode: drop the whole batch. The
                // subscriber sees a gap and backfills by `load` (§7.9) — the
                // journal is the authority (**G3**), not this delivery.
                Err(_) => return,
            }
        }
        // try_send, not send: a slow subscriber is dropped on overflow rather
        // than back-pressuring the host's forwarder (§7.9). It reconciles by seq.
        // The subscriber dropped its receiver: nothing will ever read this
        // channel again, so the sink stops rather than lingering until system
        // shutdown (the framework does not reap actors on ref drop). A `Full`
        // overflow keeps the sink alive — the subscriber reconciles by seq.
        if let Err(async_channel::TrySendError::Closed(_)) = self.tx.try_send(RecordStream {
            from: Seq::new(msg.from),
            records,
        }) {
            ctx.stop();
        }
    }
}

impl<G: Grain> Handler<CloseSink> for RecordSink<G> {
    async fn handle(&mut self, _msg: CloseSink, ctx: &Ctx<RecordSink<G>>) {
        ctx.stop();
    }
}
