//! The mailbox and its erased envelopes (spec §6).
//!
//! This is the layer **above** the [`ActorSystem`](crate::ActorSystem)
//! transport boundary where local and remote sends converge. A message becomes
//! an erased *envelope* — a closure that, given `&mut A` and its [`Ctx`], runs
//! the matching `Handler<M>` to completion. The local fast path enqueues the
//! envelope **by value, with no serialization** (spec §4.3); the remote path
//! (later) builds the same envelope after deserializing, routing the reply
//! through a `ReplyHandle` instead of a oneshot.
//!
//! Because every actor has exactly one mailbox feeding one serial executor,
//! `&mut self` in a handler is never aliased (spec §3.5, §6).

use std::sync::Arc;

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
use crate::message::Message;
use crate::reply::ReplyHandle;
use crate::runtime::BoxFuture;

/// A lending closure that runs one message: given the actor and its context, it
/// returns the future that drives the handler.
type Runner<A> = Box<dyn for<'a> FnOnce(&'a mut A, &'a Ctx<A>) -> BoxFuture<'a, ()> + Send>;

/// One unit of work queued on a mailbox: the message's manifest (for the
/// `Dispatch` event) plus the runner that executes it.
pub(crate) struct Envelope<A: Actor> {
    pub(crate) manifest: &'static str,
    pub(crate) run: Runner<A>,
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
    /// §4.4). No serialization occurs on this path.
    pub async fn ask<M>(&self, msg: M) -> Result<M::Reply, CallError>
    where
        A: Handler<M>,
        M: Message,
    {
        let manifest = M::MANIFEST.as_str();
        // `AskIssued`/`AskOutcome` always pair, so a checker can prove every ask
        // terminates and none stays pending at quiescence (invariant #1).
        self.events.emit(Event::AskIssued {
            actor: self.id.clone(),
            manifest,
        });
        let (reply_tx, reply_rx) = oneshot::channel::<M::Reply>();
        let run: Runner<A> = Box::new(move |actor, ctx| {
            Box::pin(async move {
                let reply = actor.handle(msg, ctx).await;
                let _ = reply_tx.send(reply);
            })
        });
        let result = match self.enqueue(manifest, run).await {
            // Sender dropped without replying ⇒ the actor died mid-handling.
            Ok(()) => reply_rx.await.map_err(|_| CallError::DeadLetter),
            Err(err) => Err(err),
        };
        self.events.emit(Event::AskOutcome {
            actor: self.id.clone(),
            manifest,
            failed: result.is_err(),
        });
        result
    }

    /// Fire-and-forget with backpressure: awaits mailbox space (spec §6).
    pub async fn tell<M>(&self, msg: M) -> Result<(), CallError>
    where
        A: Handler<M>,
        M: Message,
    {
        let run: Runner<A> = Box::new(move |actor, ctx| {
            Box::pin(async move {
                let _ = actor.handle(msg, ctx).await;
            })
        });
        self.enqueue(M::MANIFEST.as_str(), run).await
    }

    /// Non-blocking fire-and-forget: [`CallError::MailboxFull`] when full,
    /// rather than awaiting (spec §6).
    pub fn try_tell<M>(&self, msg: M) -> Result<(), CallError>
    where
        A: Handler<M>,
        M: Message,
    {
        let manifest = M::MANIFEST.as_str();
        let run: Runner<A> = Box::new(move |actor, ctx| {
            Box::pin(async move {
                let _ = actor.handle(msg, ctx).await;
            })
        });
        match self.sender.try_send(Envelope { manifest, run }) {
            Ok(()) => {
                self.events.emit(Event::Enqueue {
                    actor: self.id.clone(),
                    manifest,
                });
                Ok(())
            }
            Err(TrySendError::Full(_)) => Err(CallError::MailboxFull),
            Err(TrySendError::Closed(_)) => Err(CallError::DeadLetter),
        }
    }

    /// Run `f` on the actor's serial executor with `&mut A` and return its
    /// result (spec §3.5.1, `when_local`). The closure rides the mailbox like any
    /// message, so it never aliases a concurrent handler (preserves isolation).
    pub(crate) async fn run_local<F, R>(&self, f: F) -> Result<R, CallError>
    where
        F: FnOnce(&mut A) -> R + Send + 'static,
        R: Send + 'static,
    {
        let (tx, rx) = oneshot::channel::<R>();
        let run: Runner<A> = Box::new(move |actor, _ctx| {
            Box::pin(async move {
                let _ = tx.send(f(actor));
            })
        });
        self.enqueue("core.when_local", run).await?;
        rx.await.map_err(|_| CallError::DeadLetter)
    }

    async fn enqueue(&self, manifest: &'static str, run: Runner<A>) -> Result<(), CallError> {
        self.sender
            .send(Envelope { manifest, run })
            .await
            .map_err(|_| CallError::DeadLetter)?;
        self.events.emit(Event::Enqueue {
            actor: self.id.clone(),
            manifest,
        });
        Ok(())
    }

    /// Enqueue an already-decoded inbound remote message, routing its reply
    /// through `reply` (spec §4.4 inbound path). Non-blocking: returns
    /// [`CallError::MailboxFull`] under backpressure rather than stalling the
    /// receive loop. Used by the dispatch registry (see [`crate::registry`]).
    ///
    /// On a rejected enqueue the `reply` is resolved with the corresponding
    /// `CallError` so the caller never hangs. The full/closed pre-check makes
    /// the subsequent `try_send` infallible under the single-threaded simulator;
    /// a multi-threaded transport will revisit this.
    pub(crate) fn enqueue_remote<M>(&self, msg: M, reply: ReplyHandle) -> Result<(), CallError>
    where
        A: Handler<M>,
        M: Message,
    {
        if self.sender.is_closed() {
            reply.fail(CallError::DeadLetter);
            return Err(CallError::DeadLetter);
        }
        if self.sender.is_full() {
            reply.fail(CallError::MailboxFull);
            return Err(CallError::MailboxFull);
        }
        let manifest = M::MANIFEST.as_str();
        let run: Runner<A> = Box::new(move |actor, ctx| {
            Box::pin(async move {
                let outcome = actor.handle(msg, ctx).await;
                reply.send(outcome);
            })
        });
        match self.sender.try_send(Envelope { manifest, run }) {
            Ok(()) => {
                self.events.emit(Event::Enqueue {
                    actor: self.id.clone(),
                    manifest,
                });
                Ok(())
            }
            Err(TrySendError::Full(_)) => Err(CallError::MailboxFull),
            Err(TrySendError::Closed(_)) => Err(CallError::DeadLetter),
        }
    }

    /// Enqueue a [`Terminated`] death-watch signal (spec §12). It rides the same
    /// mailbox as any message, so it is observed in the actor's serial order
    /// (invariant #13).
    ///
    /// A `Terminated` MUST reach its watcher exactly once for *any* cause
    /// (invariant #11), so unlike a best-effort send this applies the §6 default
    /// backpressure policy: it **awaits** until the mailbox has room rather than
    /// dropping the signal when the queue is full. Only a *closed* mailbox lets
    /// it give up — that means the watcher itself is already gone, so there is no
    /// one left to notify.
    pub(crate) async fn enqueue_signal(&self, signal: Terminated)
    where
        A: Handler<Terminated>,
    {
        let manifest = Terminated::MANIFEST.as_str();
        let run: Runner<A> = Box::new(move |actor, ctx| {
            Box::pin(async move {
                actor.handle(signal, ctx).await;
            })
        });
        if self.sender.send(Envelope { manifest, run }).await.is_ok() {
            self.events.emit(Event::Enqueue {
                actor: self.id.clone(),
                manifest,
            });
        }
    }
}
