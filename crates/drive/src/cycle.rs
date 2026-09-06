//! The drive cycle (drive spec §3.2, §3.3, §6): reconciling the desired set
//! against what this activation has launched.
//!
//! One entry, one exit. The cycle is entered by a commit — of a command's
//! records or of a settled outcome's — by rehydration, by a backoff elapsing, or
//! by an explicit nudge; and it exits when the desired set has come to rest and
//! nothing is in flight, which is **quiescence** (§6): the grain has become an
//! ordinary waiting grain again and MAY hibernate.
//!
//! Everything the cycle holds is ephemeral activation state, rebuilt from
//! nothing on every activation and never journaled (§3.3): journaling a claim
//! would make the *launch* the durable fact, and the launch is exactly the thing
//! a crash can lose track of. The drive makes the **settled record** the durable
//! fact instead, and treats every launch as speculative.

use std::collections::BTreeSet;
use std::sync::Mutex;

use actor_core::ActorSystem;
use actor_core::Manifest;
use actor_core::Message;
use granary::GrainCtx;
use serde::Deserialize;
use serde::Serialize;

use crate::Backoff;
use crate::BackoffPolicy;
use crate::DriveEvent;
use crate::Driven;
use crate::EffectError;
use crate::Pending;
use crate::Perform;
use crate::Settled;
use crate::dangling;

/// Where an activation stands with its dangling set (§4.4): one lifecycle,
/// walked once per activation.
enum Phase<K> {
    /// No committed state seen yet: nothing is classified.
    Fresh,
    /// The dangling set's `Interrupt` members, classified at the first sight of
    /// committed state and awaiting the first drive, which resolves them.
    Classified(BTreeSet<K>),
    /// The first drive has run; every member appearing from here on is fresh.
    Driven,
}

/// Re-enter the drive cycle (spec §3.2 items 3 and 4).
///
/// An ordinary self-addressed command, and the way back into the cycle after
/// anything that commits nothing: a consumer's own event, or a backoff elapsing.
/// Like [`Settled`], a **local-only** self-tell — a consumer MUST NOT `accept`
/// it in its `GrainRegistry`, which is the deserialization allowlist for
/// *network* commands (granary §5.5). Nothing outside the grain has any business
/// injecting an effect outcome or driving another node's activation.
#[derive(Clone, Copy, Debug, Default, Serialize, Deserialize)]
pub struct Nudge;

impl Message for Nudge {
    type Reply = ();
    const MANIFEST: Manifest = Manifest::new("drive.Nudge");
}

/// The per-activation state the cycle keeps. Never journaled (§3.3, §3.4).
struct Scope<K: Ord> {
    /// Which keys this activation has launched: granary's launch-once posture
    /// (granary §7.17) with the drive's own lifetime rule. It is deliberately
    /// not granary's `LaunchGuard`, because the two release differently — the
    /// workflow facet's guard is reset wholesale when a run ends, where a claim
    /// here is dropped exactly when its key leaves the desired set (§3.2 step 2),
    /// which is what keeps the straggler filter exact.
    claims: BTreeSet<K>,
    /// Per-key consecutive unproductive settles and the delay they earn (§3.4).
    backoff: Backoff<K>,
    /// The dangling set's classification (§4.4), made at the activation's
    /// **first sight of committed state** — the first drive, or the first handler
    /// that re-enters the cycle, whichever the mailbox delivers first.
    /// Classifying from the earliest state seen is what keeps a command that
    /// arrived ahead of the rehydration nudge from having its own, fresh work
    /// re-derived as dangling.
    phase: Phase<K>,
    /// What the last cycle concluded about quiescence, so `can_passivate` — which
    /// takes no seams and so cannot read the clock — answers from a decision
    /// rather than recomputing one (§6). `false` until the first drive has run:
    /// an activation that has not resolved its dangling set may not hibernate.
    quiescent: bool,
    /// `Settled` events awaiting the commit they describe (§8). A settle that
    /// decided records is emitted from the drive that follows their commit — the
    /// nudge `settled` launches — so an append that fails and steps the
    /// activation down takes the staged event with it, and the stream never
    /// shows a `Settled { committed: true }` for records that were lost.
    staged: Vec<DriveEvent>,
}

impl<K: Ord> Scope<K> {
    fn new(policy: BackoffPolicy) -> Self {
        Scope {
            claims: BTreeSet::new(),
            backoff: Backoff::new(policy),
            phase: Phase::Fresh,
            quiescent: false,
            staged: Vec::new(),
        }
    }
}

/// The drive: one reconciliation engine, held in a driven grain's behavior
/// (spec §2.1).
///
/// Internally synchronized, so a consumer holds it as a plain field and reaches
/// it through `&self` from its handlers, `can_passivate`, and `on_passivate` —
/// the shape granary's own per-activation state already takes.
pub struct Drive<D: Driven> {
    scope: Mutex<Scope<D::Key>>,
    policy: BackoffPolicy,
}

impl<D: Driven> Default for Drive<D> {
    fn default() -> Self {
        Drive::new(BackoffPolicy::default())
    }
}

impl<D: Driven> Drive<D> {
    pub fn new(policy: BackoffPolicy) -> Self {
        Drive {
            scope: Mutex::new(Scope::new(policy)),
            policy,
        }
    }

    fn lock(&self) -> std::sync::MutexGuard<'_, Scope<D::Key>> {
        // Poisoning would mean a panic inside the cycle itself. `perform` runs
        // outside this lock and its panics are caught at the task boundary
        // (§4.3), so a poisoned lock is a bug in this crate; recovering the guard
        // keeps one bug from cascading into every later command.
        self.scope.lock().unwrap_or_else(|e| e.into_inner())
    }

    // -- the grain hooks -----------------------------------------------------

    /// Forget every claim and backoff count, arm the first drive, and nudge.
    ///
    /// A consumer calls this from `Grain::on_activate`. A re-activation starts
    /// clean by specification: the claims are ephemeral (§3.3) and the counts
    /// start at zero (§3.4). The nudge is what makes the first drive happen at
    /// all on a grain nobody is about to command — a resumed one with work still
    /// outstanding, which is the whole of resume (§4.4).
    pub fn on_activate(&self, ctx: &GrainCtx<D>) {
        *self.lock() = Scope::new(self.policy);
        // No state to classify from here (granary §4.1: `on_activate` sees no
        // state); the first handler or the first drive does that (§4.4). A fresh
        // scope has no at-rest key to wake, so the plain re-entry suffices.
        self.nudge_only(ctx);
    }

    /// Whether this activation is **quiescent** and MAY hibernate (§6, D7):
    /// nothing in flight and every member of the desired set at rest — the set
    /// empty, or its remaining members backed off to the cap.
    ///
    /// A consumer calls this from `Grain::can_passivate` and MAY narrow it
    /// further by conjoining its own condition; it never has to widen it. The
    /// harness narrows exactly once, for a turn accepted into its queue but not
    /// yet started: work owed *now* that no effect represents, so it is invisible
    /// to the desired set.
    ///
    /// It answers from the last cycle's conclusion rather than recomputing,
    /// because `can_passivate` takes no seams and so cannot read the clock. That
    /// is the same answer and a cheaper one: a cycle runs on every commit and on
    /// every backoff expiry, so the conclusion is never staler than the last
    /// thing that happened to this grain. Before the first drive has run it is
    /// `false` — an activation that has not yet resolved its dangling set has no
    /// business hibernating.
    pub fn quiescent(&self) -> bool {
        self.lock().quiescent
    }

    /// Launch a self-`tell` of [`Nudge`]: the cycle's entry point after a
    /// consumer's own commit (§3.2 item 1) or after anything that commits nothing
    /// (§3.2 item 4).
    ///
    /// A consumer calls this from every handler whose records it wants driven,
    /// with the `state` the handler was given. The tell is launched, not awaited,
    /// so it runs after the current handler's records commit — which is what makes
    /// launching effects from the resulting cycle honor the write-ahead discipline
    /// (§5) rather than race it.
    ///
    /// The state is what lets the activation's **first** handler classify the
    /// dangling set (§4.4): a handler sees the committed state *before* its own
    /// records, so on a fresh activation that is the rehydrated state exactly, and
    /// the work this command creates is never mistaken for work a previous
    /// activation left in flight.
    ///
    /// A caller-driven nudge also **wakes every at-rest key** (§3.4, §6). This is
    /// the line between the drive's own timer re-entering the cycle and someone
    /// asking: a key the drive gave up driving unattended is waiting for exactly
    /// this message, and the harness draws the same line today (harness §9.2 item
    /// 3, where the next message to drive the session re-launches an abandoned
    /// propagation).
    pub fn nudge(&self, driven: &D, state: &D::State, ctx: &GrainCtx<D>) {
        let mut scope = self.lock();
        if matches!(scope.phase, Phase::Fresh) {
            Self::classify(&mut scope, &driven.pending(state));
        }
        scope.backoff.wake();
        drop(scope);
        self.nudge_only(ctx);
    }

    /// Classify the dangling set (§4.4) if this activation has not yet: the
    /// `Interrupt` members of `desired`, the desired set at the first sight of
    /// committed state. `Reexecute` members need no record — the first drive
    /// launches them as it launches anything unclaimed.
    fn classify(scope: &mut Scope<D::Key>, desired: &[Pending<D::Key, D::Effect>]) {
        if matches!(scope.phase, Phase::Fresh) {
            scope.phase = Phase::Classified(dangling::interrupted(desired));
        }
    }

    /// Nudge without waking at-rest keys: the drive's own re-entry, from the
    /// backoff timer (§3.2 item 3).
    fn nudge_only(&self, ctx: &GrainCtx<D>) {
        let this = ctx.this();
        ctx.system().launch(Box::pin(async move {
            let _ = this.tell(Nudge).await;
        }));
    }

    // -- the cycle -----------------------------------------------------------

    /// Run one drive (spec §3.2): reconcile the desired set against what this
    /// activation has launched, and return the records the drive itself decided.
    ///
    /// The body of a consumer's `GrainHandler<Nudge>`. The drive decides no
    /// records of its own — every outcome, including a dangling `Interrupt`,
    /// reaches the journal as a [`Settled`] command — so this always returns
    /// empty. It returns `Vec<D::Event>` anyway so the handler reads like every
    /// other decision and so a later revision can journal without changing every
    /// consumer.
    pub fn advance(&self, driven: &D, state: &D::State, ctx: &GrainCtx<D>) -> Vec<D::Event> {
        let desired = driven.pending(state);
        let now = ctx.system().now();
        let mut scope = self.lock();
        // The first sight of committed state classifies the dangling set (§4.4),
        // if no handler ahead of this drive already did.
        Self::classify(&mut scope, &desired);

        // Step 2: release every claim and count whose key has left the desired
        // set. Not bookkeeping — it bounds both tables by the work outstanding
        // rather than by the work ever done, and it is what lets a consumer
        // re-request the same call site later (§3.2).
        let live: BTreeSet<D::Key> = desired.iter().map(|p| p.key.clone()).collect();
        scope.claims.retain(|k| live.contains(k));
        scope.backoff.retain_keys(|k| live.contains(k));

        // The dangling `Interrupt` members still wanted are resolved without a
        // launch (§4.4): each takes a claim — so no later drive launches it while
        // its synthesized outcome is in the mailbox — and is settled below. This
        // drive spends the classification: every later member is fresh.
        let mut interrupted = Vec::new();
        if let Phase::Classified(dangling) = std::mem::replace(&mut scope.phase, Phase::Driven) {
            for key in dangling {
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

        // Step 4: quiescence. Nothing in flight — a dangling resolution holds a
        // claim like a launch — and everything left at rest.
        let quiescent = scope.claims.is_empty() && live.iter().all(|k| scope.backoff.at_rest(k));
        let announce = quiescent && !scope.quiescent;
        scope.quiescent = quiescent;
        let staged = std::mem::take(&mut scope.staged);
        drop(scope);

        // The settles whose records this drive follows the commit of (§8): out
        // first, so on the stream every claim closes before its key can be
        // launched again.
        for event in staged {
            ctx.system().emit(event.into());
        }
        // Outside the lock: `perform` is the consumer's code and MUST NOT run
        // under the drive's mutex, or a consumer that reaches back into the drive
        // deadlocks its own activation.
        for p in launch {
            self.launch(driven, ctx, p.key, p.effect);
        }
        for key in interrupted {
            ctx.system().emit(
                DriveEvent::Interrupted {
                    grain: ctx.name().clone(),
                    key: format!("{key:?}"),
                    node: ctx.system().node(),
                }
                .into(),
            );
            self.deliver(ctx, key, Err(EffectError::Interrupted));
        }
        if announce {
            ctx.system().emit(
                DriveEvent::Quiescent {
                    grain: ctx.name().clone(),
                    node: ctx.system().node(),
                }
                .into(),
            );
        }
        Vec::new()
    }

    /// Fold one settled outcome (spec §4.2, §4.5): the body of a consumer's
    /// `GrainHandler<Settled<Self>>`.
    ///
    /// Discards a straggler — an outcome whose key has left the desired set —
    /// without calling [`Driven::settle`] and without writing any record (D5).
    /// Otherwise it hands the outcome to the consumer and returns the records the
    /// consumer decided.
    pub async fn settled(
        &self,
        driven: &D,
        state: &D::State,
        msg: Settled<D>,
        ctx: &GrainCtx<D>,
    ) -> Vec<D::Event> {
        let Settled { key, outcome } = msg;
        let desired = driven.pending(state);
        // The lock is scoped to the block so nothing holds it across the
        // consumer's `settle` below.
        let straggler = {
            let mut scope = self.lock();
            // An outcome may be the first thing an activation handles — a
            // previous activation's launch reaching this one (§3.3) — and it sees
            // the rehydrated state, so it classifies the dangling set like any
            // first handler (§4.4).
            Self::classify(&mut scope, &desired);

            // The straggler test is the desired set itself, which is why it is
            // exact and needs no separate bookkeeping: a key is in the set
            // exactly while its outcome is not yet journaled. Exactness rests on
            // keys being unique over the grain's whole journal (§3.1) — with a
            // key reused across occurrences the test would decide presence, not
            // identity.
            let straggler = !desired.iter().any(|p| p.key == key);
            if straggler {
                scope.claims.remove(&key);
            }
            straggler
        };
        if straggler {
            ctx.system().emit(
                DriveEvent::Discarded {
                    grain: ctx.name().clone(),
                    key: format!("{key:?}"),
                    node: ctx.system().node(),
                }
                .into(),
            );
            return Vec::new();
        }

        let ok = outcome.is_ok();
        let records = driven.settle(state, &key, outcome, ctx).await;
        let committed = !records.is_empty();

        // The claim is done either way: this launch has returned. What decides
        // whether the key is relaunched is the desired set on the next drive, not
        // the claim (§3.2).
        let mut scope = self.lock();
        scope.claims.remove(&key);
        let armed = if committed {
            // A productive settle clears the count; the commit itself brings the
            // next drive (§3.2 item 1).
            scope.backoff.cleared(&key);
            None
        } else {
            // Nothing committed, so nothing will re-enter the cycle on its own:
            // back the key off and arm the timer that will (§3.2 item 3, §3.4).
            // Without this the key is stranded and D2 does not hold. At the cap
            // `failed` arms nothing and the key comes to rest instead (§6).
            let jitter = ctx.system().next_random();
            scope
                .backoff
                .failed(key.clone(), ctx.system().now(), jitter)
        };
        let event = DriveEvent::Settled {
            grain: ctx.name().clone(),
            key: format!("{key:?}"),
            node: ctx.system().node(),
            ok,
            committed,
        };
        if committed {
            // Released by the drive that follows the commit (§8), not here: the
            // records are not durable yet, and an append that fails steps the
            // activation down with this still staged.
            scope.staged.push(event);
        } else {
            ctx.system().emit(event.into());
        }
        drop(scope);

        if let Some(delay) = armed {
            // The backoff timer is the cycle's entry point when nothing committed
            // (§3.2 item 3).
            let this = ctx.this();
            let system = ctx.system().clone();
            ctx.system().launch(Box::pin(async move {
                system.sleep(delay).await;
                let _ = this.tell(Nudge).await;
            }));
        } else {
            // Either the records commit and every commit re-enters the cycle
            // (§3.2 item 1) — the tell is launched, so it runs after this
            // handler's records commit, and a consumer wires nothing beyond
            // forwarding the two commands — or the key just came to rest, which
            // may have been the last thing holding the activation resident, and
            // nothing else would re-enter the cycle to conclude quiescence.
            self.nudge_only(ctx);
        }
        records
    }

    // -- launching -----------------------------------------------------------

    /// Hand one effect to the spawner and return (§3.2). The drive MUST NOT await
    /// a launch: the activation therefore never blocks, which is what lets a
    /// grain with a hundred outstanding effects hold no thread and, once
    /// quiescent, no memory.
    fn launch(&self, driven: &D, ctx: &GrainCtx<D>, key: D::Key, effect: D::Effect) {
        let cx = Perform::new(ctx.name().clone(), ctx.system().clone());
        // `perform` runs outside the activation, so its future is `'static` and
        // holds no borrow of the grain: the consumer cloned what it needs out of
        // `&self` before returning it.
        let fut = driven.perform(effect, cx);
        ctx.system().emit(
            DriveEvent::Launched {
                grain: ctx.name().clone(),
                key: format!("{key:?}"),
                node: ctx.system().node(),
            }
            .into(),
        );
        let this = ctx.this();
        ctx.system().launch(Box::pin(async move {
            let outcome = match dangling::catch(fut).await {
                Ok(outcome) => outcome,
                Err(why) => Err(EffectError::Failed(why)),
            };
            let _ = this.tell(Settled::<D> { key, outcome }).await;
        }));
    }

    /// Self-`tell` a synthesized outcome: the path a dangling `Interrupt` takes to
    /// the command barrier (§3.2, §4.4).
    fn deliver(&self, ctx: &GrainCtx<D>, key: D::Key, outcome: Result<D::Outcome, EffectError>) {
        let this = ctx.this();
        ctx.system().launch(Box::pin(async move {
            let _ = this.tell(Settled::<D> { key, outcome }).await;
        }));
    }
}
