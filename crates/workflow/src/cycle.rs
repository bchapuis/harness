//! The workflow cycle (workflow spec §4.2, §4.3, §7): reconciling the desired set
//! against what this activation has launched.
//!
//! One entry, one exit. The cycle is entered by a commit (of a command's
//! records or of a settled outcome's) by rehydration, by a backoff elapsing, or
//! by an explicit tick; and it exits when the desired set has come to rest and
//! nothing is in flight, which is **quiescence** (§7): the entity has become an
//! ordinary waiting entity again and MAY hibernate.
//!
//! Everything the cycle holds is ephemeral activation state, rebuilt from
//! nothing on every activation and never journaled (§4.3): journaling a claim
//! would make the *launch* the durable fact, and the launch is exactly the thing
//! a crash can lose track of. The engine makes the **settled record** the durable
//! fact instead, and treats every launch as speculative.

use std::collections::BTreeSet;
use std::sync::Mutex;

use actor_core::ActorSystem;
use actor_core::Manifest;
use actor_core::Message;
use entity::EntityCtx;
use serde::Deserialize;
use serde::Serialize;

use crate::backoff::Backoff;
use crate::EffectError;
use crate::Pending;
use crate::Perform;
use crate::Settled;
use crate::Workflow;
use crate::WorkflowEvent;
use crate::orphan;

/// Where an activation stands with its orphan set (§5.3): one lifecycle,
/// walked once per activation.
enum Phase<K> {
    /// No committed state seen yet: nothing is classified.
    Fresh,
    /// The orphan set's `Interrupt` members, classified at the first sight of
    /// committed state and awaiting the first tick, which resolves them.
    Classified(BTreeSet<K>),
    /// The first tick has run; every member appearing from here on is fresh.
    Ticked,
}

/// Re-enter the workflow cycle (spec §4.2 items 3 and 4).
///
/// An ordinary self-addressed command, and the way back into the cycle after
/// anything that commits nothing: a consumer's own event, or a backoff elapsing.
/// Like [`Settled`], a **local-only** self-tell: a consumer MUST NOT `accept`
/// it in its `EntityRegistry`, which is the deserialization allowlist for
/// *network* commands (entity §5.5). Nothing outside the entity has any business
/// injecting an effect outcome or driving another node's activation.
#[derive(Clone, Copy, Debug, Default, Serialize, Deserialize)]
pub struct Tick;

impl Message for Tick {
    type Reply = ();
    const MANIFEST: Manifest = Manifest::new("workflow.Tick");
}

/// The per-activation state the cycle keeps. Never journaled (§4.3, §4.4).
struct Scope<K: Ord> {
    /// Which keys this activation has launched: entity's launch-once posture
    /// (facets §5.6) with the engine's own lifetime rule. It is deliberately
    /// not entity's `LaunchGuard`, because the two release differently: the
    /// steps facet's guard is reset wholesale when a run ends, where a claim
    /// here is dropped exactly when its key leaves the desired set (§4.2 step 2),
    /// which is what keeps the straggler filter exact.
    claims: BTreeSet<K>,
    /// Per-key consecutive unproductive settles and the delay they earn (§4.4).
    backoff: Backoff<K>,
    /// The orphan set's classification (§5.3), made at the activation's
    /// **first sight of committed state**: the first tick, or the first handler
    /// that re-enters the cycle, whichever the mailbox delivers first.
    /// Classifying from the earliest state seen is what keeps a command that
    /// arrived ahead of the rehydration tick from having its own, fresh work
    /// re-derived as orphaned.
    phase: Phase<K>,
    /// What the last cycle concluded about quiescence, so `can_hibernate` (which
    /// takes no seams and so cannot read the clock) answers from a decision
    /// rather than recomputing one (§7). `false` until the first tick has run:
    /// an activation that has not resolved its orphan set may not hibernate.
    quiescent: bool,
    /// `Settled` events awaiting the commit they describe (§10). A settle that
    /// decided records is emitted from the engine that follows their commit (the
    /// tick `settled` launches), so an append that fails and steps the
    /// activation down takes the staged event with it, and the stream never
    /// shows a `Settled { committed: true }` for records that were lost.
    staged: Vec<WorkflowEvent>,
}

impl<K: Ord> Scope<K> {
    fn new() -> Self {
        Scope {
            claims: BTreeSet::new(),
            backoff: Backoff::default(),
            phase: Phase::Fresh,
            quiescent: false,
            staged: Vec::new(),
        }
    }
}

/// The engine: one reconciliation engine, held in a workflow entity's behavior
/// (spec §3.2).
///
/// Internally synchronized, so a consumer holds it as a plain field and reaches
/// it through `&self` from its handlers and `can_hibernate`: the shape entity's
/// own per-activation state already takes.
pub struct Engine<D: Workflow> {
    scope: Mutex<Scope<D::Key>>,
}

impl<D: Workflow> Default for Engine<D> {
    fn default() -> Self {
        Engine {
            scope: Mutex::new(Scope::new()),
        }
    }
}

impl<D: Workflow> Engine<D> {
    fn lock(&self) -> std::sync::MutexGuard<'_, Scope<D::Key>> {
        // Poisoning would mean a panic inside the cycle itself. `perform` runs
        // outside this lock and its panics are caught at the task boundary
        // (§5.2), so a poisoned lock is a bug in this crate; recovering the guard
        // keeps one bug from cascading into every later command.
        self.scope.lock().unwrap_or_else(|e| e.into_inner())
    }

    // -- the entity hooks -----------------------------------------------------

    /// Forget every claim and backoff count, arm the first tick, and tick.
    ///
    /// A consumer calls this from `Entity::on_activate`. A re-activation starts
    /// clean by specification: the claims are ephemeral (§4.3) and the counts
    /// start at zero (§4.4). The tick is what makes the first tick happen at
    /// all on an entity nobody is about to command: a resumed one with work still
    /// outstanding, which is the whole of resume (§5.3).
    pub fn on_activate(&self, ctx: &EntityCtx<D>) {
        *self.lock() = Scope::new();
        // No state to classify from here (entity §4.1: `on_activate` sees no
        // state); the first handler or the first tick does that (§5.3). A fresh
        // scope has no at-rest key to wake, so the plain re-entry suffices.
        self.tick_only(ctx);
    }

    /// Whether this activation is **quiescent** and MAY hibernate (§7, W7):
    /// nothing in flight and every member of the desired set at rest; the set
    /// empty, or its remaining members backed off to the cap.
    ///
    /// A consumer calls this from `Entity::can_hibernate` and MAY narrow it
    /// further by conjoining its own condition; it never has to widen it. The
    /// agent runtime narrows it twice: for a turn accepted into its queue but
    /// not yet started (work owed *now* that no effect represents, so invisible
    /// to the desired set), and for a [`Binding`](crate::Binding) mid-open,
    /// which is a component the engine never sees.
    ///
    /// It answers from the last cycle's conclusion rather than recomputing,
    /// because `can_hibernate` takes no seams and so cannot read the clock. That
    /// is the same answer and a cheaper one: a cycle runs on every commit and on
    /// every backoff expiry, so the conclusion is never staler than the last
    /// thing that happened to this entity. Before the first tick has run it is
    /// `false`: an activation that has not yet resolved its orphan set has no
    /// business hibernating.
    pub fn quiescent(&self) -> bool {
        self.lock().quiescent
    }

    /// Launch a self-`tell` of [`Tick`]: the cycle's entry point after a
    /// consumer's own commit (§4.2 item 1) or after anything that commits nothing
    /// (§4.2 item 4).
    ///
    /// A consumer calls this from every handler whose records it wants driven,
    /// with the `state` the handler was given. The tell is launched, not awaited,
    /// so it runs after the current handler's records commit, which is what makes
    /// launching effects from the resulting cycle honor the write-ahead discipline
    /// (§6) rather than race it.
    ///
    /// The state is what lets the activation's **first** handler classify the
    /// orphan set (§5.3): a handler sees the committed state *before* its own
    /// records, so on a fresh activation that is the rehydrated state exactly, and
    /// the work this command creates is never mistaken for work a previous
    /// activation left in flight.
    ///
    /// A caller-driven tick also **wakes every at-rest key** (§4.4, §7). This is
    /// the line between the engine's own timer re-entering the cycle and someone
    /// asking: a key the engine gave up driving unattended is waiting for exactly
    /// this message, and the agent runtime draws the same line (agent §9.2 item 3,
    /// where the next message to drive the session re-launches an abandoned
    /// propagation).
    pub fn tick(&self, workflow: &D, state: &D::State, ctx: &EntityCtx<D>) {
        let mut scope = self.lock();
        if matches!(scope.phase, Phase::Fresh) {
            Self::classify(&mut scope, &workflow.pending(state));
        }
        scope.backoff.wake();
        drop(scope);
        self.tick_only(ctx);
    }

    /// Classify the orphan set (§5.3) if this activation has not yet: the
    /// `Interrupt` members of `desired`, the desired set at the first sight of
    /// committed state. `Reexecute` members need no record: the first tick
    /// launches them as it launches anything unclaimed.
    fn classify(scope: &mut Scope<D::Key>, desired: &[Pending<D::Key, D::Effect>]) {
        if matches!(scope.phase, Phase::Fresh) {
            scope.phase = Phase::Classified(orphan::interrupted(desired));
        }
    }

    /// Tick without waking at-rest keys: the engine's own re-entry, from the
    /// backoff timer (§4.2 item 3).
    fn tick_only(&self, ctx: &EntityCtx<D>) {
        let this = ctx.this();
        ctx.system().launch(Box::pin(async move {
            let _ = this.tell(Tick).await;
        }));
    }

    // -- the cycle -----------------------------------------------------------

    /// Run one tick (spec §4.2): reconcile the desired set against what this
    /// activation has launched, and return the records the engine itself decided.
    ///
    /// The body of a consumer's `EntityHandler<Tick>`. The engine decides no
    /// records of its own (every outcome, including an orphaned `Interrupt`,
    /// reaches the journal as a [`Settled`] command), so this always returns
    /// empty. It returns `Vec<D::Event>` anyway so the handler reads like every
    /// other decision and so a later revision can journal without changing every
    /// consumer.
    pub fn advance(&self, workflow: &D, state: &D::State, ctx: &EntityCtx<D>) -> Vec<D::Event> {
        let desired = workflow.pending(state);
        let now = ctx.system().now();
        let mut scope = self.lock();
        // The first sight of committed state classifies the orphan set (§5.3),
        // if no handler ahead of this tick already did.
        Self::classify(&mut scope, &desired);

        // Step 2: release every claim and count whose key has left the desired
        // set. Not bookkeeping: it bounds both tables by the work outstanding
        // rather than by the work ever done, and it is what lets a consumer
        // re-request the same call site later (§4.2).
        let live: BTreeSet<D::Key> = desired.iter().map(|p| p.key.clone()).collect();
        scope.claims.retain(|k| live.contains(k));
        scope.backoff.retain_keys(|k| live.contains(k));

        // The orphaned `Interrupt` members still wanted are resolved without a
        // launch (§5.3): each takes a claim (so no later tick launches it while
        // its synthesized outcome is in the mailbox), and is settled below. This
        // tick spends the classification: every later member is fresh.
        let mut interrupted = Vec::new();
        if let Phase::Classified(orphaned) = std::mem::replace(&mut scope.phase, Phase::Ticked) {
            for key in orphaned {
                if live.contains(&key) && scope.claims.insert(key.clone()) {
                    interrupted.push(key);
                }
            }
        }

        // Step 3: launch every member this activation has not claimed and whose
        // backoff has elapsed.
        let mut launch = Vec::new();
        for p in desired {
            if scope.claims.contains(&p.key) || !scope.backoff.ready(&p.key, now) {
                continue;
            }
            scope.claims.insert(p.key.clone());
            launch.push(p);
        }

        // Step 4: quiescence. Nothing in flight (an orphaned resolution holds a
        // claim like a launch), and everything left at rest.
        let quiescent = scope.claims.is_empty() && live.iter().all(|k| scope.backoff.at_rest(k));
        let announce = quiescent && !scope.quiescent;
        scope.quiescent = quiescent;
        let staged = std::mem::take(&mut scope.staged);
        drop(scope);

        // The settles whose records this tick follows the commit of (§10): out
        // first, so on the stream every claim closes before its key can be
        // launched again.
        for event in staged {
            ctx.system().emit(event.into());
        }
        // Outside the lock: `perform` is the consumer's code and MUST NOT run
        // under the engine's mutex, or a consumer that reaches back into the engine
        // deadlocks its own activation.
        for p in launch {
            self.launch(workflow, ctx, p.key, p.effect);
        }
        for key in interrupted {
            ctx.system().emit(
                WorkflowEvent::Interrupted {
                    entity: ctx.name().clone(),
                    key: format!("{key:?}"),
                    node: ctx.system().node(),
                }
                .into(),
            );
            self.deliver(ctx, key, Err(EffectError::Interrupted));
        }
        if announce {
            ctx.system().emit(
                WorkflowEvent::Quiescent {
                    entity: ctx.name().clone(),
                    node: ctx.system().node(),
                }
                .into(),
            );
        }
        Vec::new()
    }

    /// Fold one settled outcome (spec §5.1, §5.4): the body of a consumer's
    /// `EntityHandler<Settled<Self>>`.
    ///
    /// Discards a straggler (an outcome whose key has left the desired set)
    /// without calling [`Workflow::settle`] and without writing any record (W5).
    /// Otherwise it hands the outcome to the consumer and returns the records the
    /// consumer decided.
    ///
    /// Then it re-enters the cycle itself, so a consumer wires nothing more. When
    /// the settle decided no records, nothing would re-enter on its own: the key is
    /// backed off, and a task launched through the `Spawner` sleeps the delay on
    /// the `Clock` and then self-tells a [`Tick`] (§4.2 item 3, §4.4). Otherwise
    /// (the records are about to commit, or the key has just reached the cap and
    /// come to rest) it self-tells a [`Tick`] at once, which the entity handles
    /// after this handler's records commit; at the cap that tick is what concludes
    /// quiescence. A `Settled` event for records is staged for that tick to
    /// release, after the commit (§10).
    pub async fn settled(
        &self,
        workflow: &D,
        state: &D::State,
        msg: Settled<D>,
        ctx: &EntityCtx<D>,
    ) -> Vec<D::Event> {
        let Settled { key, outcome } = msg;
        let desired = workflow.pending(state);
        // The lock is scoped to the block so nothing holds it across the
        // consumer's `settle` below.
        let straggler = {
            let mut scope = self.lock();
            // An outcome may be the first thing an activation handles: a
            // previous activation's launch reaching this one (§4.3), and it sees
            // the rehydrated state, so it classifies the orphan set like any
            // first handler (§5.3).
            Self::classify(&mut scope, &desired);

            // The straggler test is the desired set itself, which is why it is
            // exact and needs no separate bookkeeping: a key is in the set
            // exactly while its outcome is not yet journaled. Exactness rests on
            // keys being unique over the entity's whole journal (§4.1): with a
            // key reused across occurrences, the test would decide presence, not
            // identity.
            let straggler = !desired.iter().any(|p| p.key == key);
            if straggler {
                scope.claims.remove(&key);
            }
            straggler
        };
        if straggler {
            ctx.system().emit(
                WorkflowEvent::Discarded {
                    entity: ctx.name().clone(),
                    key: format!("{key:?}"),
                    node: ctx.system().node(),
                }
                .into(),
            );
            return Vec::new();
        }

        let ok = outcome.is_ok();
        let records = workflow.settle(state, &key, outcome, ctx).await;
        let committed = !records.is_empty();

        // The claim is done either way: this launch has returned. What decides
        // whether the key is relaunched is the desired set on the next tick, not
        // the claim (§4.2).
        let mut scope = self.lock();
        scope.claims.remove(&key);
        let armed = if committed {
            // A productive settle clears the count; the commit itself brings the
            // next tick (§4.2 item 1).
            scope.backoff.cleared(&key);
            None
        } else {
            // Nothing committed, so nothing will re-enter the cycle on its own:
            // back the key off and arm the timer that will (§4.2 item 3, §4.4).
            // Without this the key is stranded and W2 does not hold. At the cap
            // `failed` arms nothing and the key comes to rest instead (§7).
            let jitter = ctx.system().next_random();
            scope
                .backoff
                .failed(key.clone(), ctx.system().now(), jitter)
        };
        let event = WorkflowEvent::Settled {
            entity: ctx.name().clone(),
            key: format!("{key:?}"),
            node: ctx.system().node(),
            ok,
            committed,
        };
        if committed {
            // Released by the engine that follows the commit (§10), not here: the
            // records are not durable yet, and an append that fails steps the
            // activation down with this still staged.
            scope.staged.push(event);
        } else {
            ctx.system().emit(event.into());
        }
        drop(scope);

        if let Some(delay) = armed {
            // The backoff timer is the cycle's entry point when nothing committed
            // (§4.2 item 3).
            let this = ctx.this();
            let system = ctx.system().clone();
            ctx.system().launch(Box::pin(async move {
                system.sleep(delay).await;
                let _ = this.tell(Tick).await;
            }));
        } else {
            // Either the records commit and every commit re-enters the cycle
            // (§4.2 item 1), the tell being launched so it runs after this
            // handler's records commit, or the key just came to rest, which may
            // have been the last thing holding the activation resident, and
            // nothing else would re-enter the cycle to conclude quiescence.
            self.tick_only(ctx);
        }
        records
    }

    // -- launching -----------------------------------------------------------

    /// Hand one effect to the spawner and return (§4.2). The engine MUST NOT await
    /// a launch: the activation therefore never blocks, which is what lets an
    /// entity with a hundred outstanding effects hold no thread and, once
    /// quiescent, no memory.
    fn launch(&self, workflow: &D, ctx: &EntityCtx<D>, key: D::Key, effect: D::Effect) {
        let cx = Perform::new(ctx.name().clone(), ctx.system().clone());
        // `perform` runs outside the activation, so its future is `'static` and
        // holds no borrow of the entity: the consumer cloned what it needs out of
        // `&self` before returning it.
        let fut = workflow.perform(effect, cx);
        ctx.system().emit(
            WorkflowEvent::Launched {
                entity: ctx.name().clone(),
                key: format!("{key:?}"),
                node: ctx.system().node(),
            }
            .into(),
        );
        let this = ctx.this();
        ctx.system().launch(Box::pin(async move {
            let outcome = match orphan::catch(fut).await {
                Ok(outcome) => outcome,
                Err(why) => Err(EffectError::Failed(why)),
            };
            let _ = this.tell(Settled::<D> { key, outcome }).await;
        }));
    }

    /// Self-`tell` a synthesized outcome: the path an orphaned `Interrupt` takes to
    /// the command barrier (§4.2, §5.3).
    fn deliver(&self, ctx: &EntityCtx<D>, key: D::Key, outcome: Result<D::Outcome, EffectError>) {
        let this = ctx.this();
        ctx.system().launch(Box::pin(async move {
            let _ = this.tell(Settled::<D> { key, outcome }).await;
        }));
    }
}
