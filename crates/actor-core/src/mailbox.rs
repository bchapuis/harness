//! The mailbox and its erased envelopes (spec §6).
//!
//! This is the layer **above** the [`ActorSystem`](crate::ActorSystem)
//! transport boundary where local and remote sends converge. A message becomes
//! an erased *envelope*: a closure that, given `&mut A` and its [`Ctx`], runs
//! the matching `Handler<M>` to completion. The local fast path enqueues the
//! envelope **by value, with no serialization** (spec §4.3); the remote path
//! builds the same envelope after deserializing, routing the reply through a
//! `ReplyHandle` instead of a oneshot.
//!
//! Because every actor has exactly one mailbox feeding one serial executor,
//! `&mut self` in a handler is never aliased (spec §3.5, §6).

use std::sync::Arc;
use std::time::Duration;

use async_channel::Receiver;
use async_channel::Sender;
use async_channel::TrySendError;
use futures::channel::oneshot;

use crate::actor::Actor;
use crate::actor::Handler;
use crate::actor::Terminated;
use crate::context::Ctx;
use crate::error::CallError;
use crate::event::Event;
use crate::event::EventSink;
use crate::id::ActorId;
use crate::message::Manifest;
use crate::message::Message;
use crate::reply::ReplyHandle;
use crate::runtime::BoxFuture;
use crate::runtime::Clock;

/// A lending closure that runs one message: given the actor, its context, and
/// the envelope's reply handle (`None` on the local path), it returns the
/// future that drives the handler.
type Runner<A> =
    Box<dyn for<'a> FnOnce(&'a mut A, &'a Ctx<A>, Option<ReplyHandle>) -> BoxFuture<'a, ()> + Send>;

/// The fire-and-forget runner shared by [`Mailbox::tell`], [`Mailbox::try_tell`],
/// and the death-watch signal: drives the handler to completion and discards the
/// reply.
fn tell_runner<A, M>(msg: M) -> Runner<A>
where
    A: Actor + Handler<M>,
    M: Message,
{
    Box::new(move |actor, ctx, _reply| {
        Box::pin(async move {
            let _ = actor.handle(msg, ctx).await;
        })
    })
}

/// One unit of work queued on a mailbox: the message's manifest (for the
/// `Dispatch` event) plus the runner that executes it. The **local, by-value**
/// envelope (spec §6): a live closure, not serialized bytes.
pub(crate) struct Envelope<A: Actor> {
    pub(crate) manifest: Manifest,
    pub(crate) run: Runner<A>,
    /// The reply side of an inbound remote `ask`, carried beside the runner
    /// rather than captured inside it: a rejected enqueue hands the envelope
    /// back, and the handle must still be reachable to fail it with the reason
    /// (spec §4.5: exactly one resolution per `ask`).
    pub(crate) reply: Option<ReplyHandle>,
}

impl<A: Actor> Envelope<A> {
    /// A local envelope: no reply handle, the caller awaits its own oneshot.
    fn local(manifest: Manifest, run: Runner<A>) -> Envelope<A> {
        Envelope {
            manifest,
            run,
            reply: None,
        }
    }

    /// Resolve a rejected envelope's reply with `error`, and hand the error
    /// back for the caller's own result.
    fn refuse(self, error: CallError) -> CallError {
        if let Some(reply) = self.reply {
            reply.fail(error.clone());
        }
        error
    }
}

/// The consuming end of a mailbox, owned by the actor's executor.
pub(crate) type Inbox<A> = Receiver<Envelope<A>>;

/// The bounded queue feeding an actor's serial executor (spec §6). Cloning
/// yields another producer handle to the same queue; cheap (an `Arc` inside the
/// channel sender).
pub struct Mailbox<A: Actor> {
    id: ActorId,
    sender: Sender<Envelope<A>>,
    events: Arc<dyn EventSink>,
}

impl<A: Actor> Clone for Mailbox<A> {
    fn clone(&self) -> Self {
        Mailbox {
            id: self.id.clone(),
            sender: self.sender.clone(),
            events: Arc::clone(&self.events),
        }
    }
}

impl<A: Actor> Mailbox<A> {
    /// Create a mailbox and its paired inbox with a bounded capacity.
    pub(crate) fn channel(
        id: ActorId,
        capacity: usize,
        events: Arc<dyn EventSink>,
    ) -> (Mailbox<A>, Inbox<A>) {
        let (sender, inbox) = async_channel::bounded(capacity);
        (Mailbox { id, sender, events }, inbox)
    }

    /// Request/response: enqueue `msg` by value and await its typed reply (spec
    /// §4.4), failing with [`CallError::Timeout`] once `within` of `clock`'s
    /// time has elapsed (spec §14.2). No serialization occurs on this path.
    /// Emits no `AskIssued`/`AskOutcome`: [`ActorRef::ask`](crate::ActorRef::ask)
    /// brackets the call.
    pub async fn ask<M, C>(
        &self,
        msg: M,
        clock: &C,
        within: Duration,
    ) -> Result<M::Reply, CallError>
    where
        A: Handler<M>,
        M: Message,
        C: Clock,
    {
        let (reply_tx, reply_rx) = oneshot::channel::<M::Reply>();
        let run: Runner<A> = Box::new(move |actor, ctx, _reply| {
            Box::pin(async move {
                let reply = actor.handle(msg, ctx).await;
                let _ = reply_tx.send(reply);
            })
        });
        let call = async {
            match self.enqueue(Envelope::local(M::MANIFEST, run)).await {
                // Sender dropped without replying ⇒ the actor died mid-handling.
                Ok(()) => reply_rx.await.map_err(|_| CallError::DeadLetter),
                Err(err) => Err(err),
            }
        };
        // The deadline covers the whole call, waiting for mailbox space as much
        // as the reply, so a timed-out ask still resolves and its caller's
        // bracket closes (invariant #1). A handler still running when it elapses
        // finishes on its own; its reply has no taker.
        match clock.timeout(within, call).await {
            Ok(result) => result,
            Err(_elapsed) => Err(CallError::Timeout),
        }
    }

    /// Fire-and-forget with backpressure: awaits mailbox space (spec §6).
    pub async fn tell<M>(&self, msg: M) -> Result<(), CallError>
    where
        A: Handler<M>,
        M: Message,
    {
        self.enqueue(Envelope::local(M::MANIFEST, tell_runner(msg)))
            .await
    }

    /// Non-blocking fire-and-forget: [`CallError::MailboxFull`] when full,
    /// rather than awaiting (spec §6).
    pub fn try_tell<M>(&self, msg: M) -> Result<(), CallError>
    where
        A: Handler<M>,
        M: Message,
    {
        self.try_enqueue(Envelope::local(M::MANIFEST, tell_runner(msg)))
    }

    /// Awaiting enqueue (spec §6 default backpressure): blocks until the mailbox
    /// has room, failing only on a closed mailbox. Shared by `ask`, `tell`, and
    /// the death-watch signal.
    async fn enqueue(&self, envelope: Envelope<A>) -> Result<(), CallError> {
        let manifest = envelope.manifest;
        self.sender
            .send(envelope)
            .await
            .map_err(|rejected| rejected.0.refuse(CallError::DeadLetter))?;
        self.emit_enqueue(manifest);
        Ok(())
    }

    /// Non-blocking enqueue: returns [`CallError::MailboxFull`] when the queue is
    /// full rather than awaiting. Shared by `try_tell` (local best-effort) and
    /// `enqueue_remote` (the receive loop, which must not stall on backpressure).
    /// A rejected envelope's reply handle, if any, is resolved with the same
    /// error the caller gets, so a remote asker sees `MailboxFull` rather than
    /// a cancelled channel.
    fn try_enqueue(&self, envelope: Envelope<A>) -> Result<(), CallError> {
        let manifest = envelope.manifest;
        match self.sender.try_send(envelope) {
            Ok(()) => {
                self.emit_enqueue(manifest);
                Ok(())
            }
            Err(TrySendError::Full(rejected)) => Err(rejected.refuse(CallError::MailboxFull)),
            Err(TrySendError::Closed(rejected)) => Err(rejected.refuse(CallError::DeadLetter)),
        }
    }

    /// Record that an envelope reached the queue (spec §16). Both enqueue paths
    /// emit exactly one `Enqueue` here on success, so the event has a single
    /// origin.
    fn emit_enqueue(&self, manifest: Manifest) {
        self.events.emit(Event::Enqueue {
            actor: self.id.clone(),
            manifest,
        });
    }

    /// Enqueue an already-decoded inbound remote message, routing its reply
    /// through `reply` (spec §4.4 inbound path). Non-blocking: returns
    /// [`CallError::MailboxFull`] under backpressure rather than stalling the
    /// receive loop. Used by the dispatch registry (see [`crate::registry`]).
    ///
    /// On a rejected enqueue the `reply` is resolved with the corresponding
    /// `CallError` so the caller never hangs.
    pub(crate) fn enqueue_remote<M>(&self, msg: M, reply: ReplyHandle) -> Result<(), CallError>
    where
        A: Handler<M>,
        M: Message,
    {
        // The executor hands the envelope's reply handle to the runner, so the
        // handle rides beside the closure where a rejected enqueue can reach it.
        let run: Runner<A> = Box::new(move |actor, ctx, reply| {
            Box::pin(async move {
                let outcome = actor.handle(msg, ctx).await;
                if let Some(reply) = reply {
                    reply.send(outcome);
                }
            })
        });
        self.try_enqueue(Envelope {
            manifest: M::MANIFEST,
            run,
            reply: Some(reply),
        })
    }

    /// Enqueue a [`Terminated`] death-watch signal (spec §12). It rides the same
    /// mailbox as any message, so it is observed in the actor's serial order
    /// (invariant #13).
    ///
    /// A `Terminated` MUST reach its watcher exactly once for *any* cause
    /// (invariant #11), so this applies the §6 default backpressure policy: it
    /// **awaits** until the mailbox has room rather than dropping the signal.
    /// Only a *closed* mailbox lets it give up: the watcher itself is already
    /// gone, so there is no one left to notify.
    pub(crate) async fn enqueue_signal(&self, signal: Terminated)
    where
        A: Handler<Terminated>,
    {
        let manifest = Terminated::MANIFEST;
        // Capture the signal's facts before it moves into the handler closure,
        // to mark the delivery on the event stream once it lands.
        let target = signal.id.clone();
        let reason = signal.reason;
        if self
            .enqueue(Envelope::local(manifest, tell_runner(signal)))
            .await
            .is_ok()
        {
            // The watcher's mailbox is the one and only point a `Terminated` is
            // actually *delivered* (spec §12, §16), so the event is emitted here:
            // a forward to another node is not a delivery, and a watch-after-death
            // delivery is still counted.
            self.events.emit(Event::TerminatedDelivered {
                target,
                watcher: self.id.clone(),
                reason,
            });
        }
    }
}
