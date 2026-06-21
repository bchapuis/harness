//! The host actor: the durability protocol, rehydration, and hibernation
//! (spec §6, §9, §10).
//!
//! A grain's live activation is a host actor. It holds the grain behavior, the
//! folded state, and the committed head, and runs the §6 per-command protocol on
//! its serial executor. Two gate guarantees fall out of that serial executor at
//! no cost:
//!
//! - **Input gate** (§6): the executor runs each handler future to completion
//!   before pulling the next message, and the protocol mutates state only *after*
//!   the commit with no `.await` between the fold and the reply — so no second
//!   command ever observes half-committed state.
//! - **Output gate** (§6): the reply is simply the value the handler returns
//!   *after* the commit await; a failed commit returns a `GrainError` instead, so
//!   no observer is ever told an effect happened that did not durably happen
//!   (invariant **G5**).
//!
//! The host appends through the [`GrainJournal`] seam, so the same protocol runs over
//! the Tier-1 memory journal and (later) the Tier-2 sharded-Raft journal.

use actor_core::Actor;
use actor_core::ActorRef;
use actor_core::ActorSystem;
use actor_core::BoxError;
use actor_core::CallError;
use actor_core::Ctx;
use actor_core::Handler;
use actor_core::HandlerRegistry;
use actor_core::Manifest;
use actor_core::Message;
use serde::Deserialize;
use serde::Serialize;

use crate::config::GranaryConfig;
use crate::error::GrainError;
use crate::event::GrainEvent;
use crate::gateway::Gateway;
use crate::grain::Grain;
use crate::grain::GrainCtx;
use crate::grain::GrainHandler;
use crate::grain::GrainName;
use crate::grain::GrainRegistry;
use std::sync::Arc;

use crate::journal::AppendOutcome;
use crate::journal::DynGrainJournal;
use crate::journal::Seq;
use crate::system::GranarySystem;

/// How many events a single rehydration read pulls from the journal at a time.
const REPLAY_BATCH: usize = 256;

/// The internal command carrying a typed message to the host on the **local fast
/// path** (§5.4). Its reply is `Result<M::Reply, GrainError>`: the durability
/// outcome wraps the user's application reply, keeping the two failure layers
/// distinct (§4.2, §12). Not in the host's network `register` list, so it travels
/// only by value through a local `ask` (never serialized).
#[derive(Serialize, Deserialize)]
#[serde(transparent)]
pub(crate) struct RunTyped<M>(pub(crate) M);

impl<M: Message> Message for RunTyped<M> {
    type Reply = Result<M::Reply, GrainError>;
    // The inner manifest is the dispatch key (§5.5); the wrapper adds none.
    const MANIFEST: Manifest = M::MANIFEST;
}

/// An internal self-tick that drives idle eviction (§10): the host checks how
/// long it has been idle and either hibernates or reschedules.
#[derive(Serialize, Deserialize)]
pub(crate) struct CheckIdle;

impl Message for CheckIdle {
    type Reply = ();
    const MANIFEST: Manifest = Manifest::new("granary.CheckIdle");
}

/// A grain's live activation (spec §10): state folded from the journal, plus the
/// machinery of the §6 durability protocol. Disposable — rebuilt from the journal
/// on the next activation (invariant **G3**).
pub struct Host<G: Grain> {
    grain: G,
    state: G::State,
    /// The grain's committed head. Set **only** from journal/snapshot returns,
    /// never trusted across an activation (invariant **G3**).
    head: Seq,
    /// The seq of the latest persisted snapshot, to decide when to snapshot again.
    last_snapshot: Seq,
    /// The runtime type name (spec §5.1), `G::GRAIN_TYPE` by default; a
    /// caller-supplied name under [`granary_named`](crate::GranaryExt::granary_named).
    /// Threaded into [`GrainCtx`] so a self-reference resolves under the right type.
    grain_type: &'static str,
    name: GrainName,
    journal: Arc<dyn DynGrainJournal>,
    config: GranaryConfig,
    gateway: ActorRef<Gateway<G>>,
    /// Virtual time of the last *command* (not a tick), for idle eviction (§10).
    last_active: actor_core::Instant,
}

impl<G: Grain> Host<G> {
    /// Build a fresh, un-rehydrated activation. Rehydration happens in
    /// [`Actor::started`], where the system (and thus the journal's authoritative
    /// head) is reachable.
    pub(crate) fn new(
        grain_type: &'static str,
        grain: G,
        name: GrainName,
        journal: Arc<dyn DynGrainJournal>,
        config: GranaryConfig,
        gateway: ActorRef<Gateway<G>>,
    ) -> Host<G> {
        Host {
            grain,
            state: G::State::default(),
            head: Seq::ZERO,
            last_snapshot: Seq::ZERO,
            grain_type,
            name,
            journal,
            config,
            gateway,
            last_active: actor_core::Instant::ZERO,
        }
    }

    /// Build the handler/lifecycle context for the grain (§4.3).
    fn grain_ctx(&self, ctx: &Ctx<Host<G>>) -> GrainCtx<G> {
        GrainCtx::new(
            self.grain_type,
            self.name.clone(),
            ctx.system().clone(),
            self.gateway.clone(),
        )
    }

    /// Rebuild state from the journal (spec §9): load the latest snapshot, then
    /// replay the events after it, folding with `apply`. The head is taken from
    /// the journal's authority (**G3**); a snapshot whose seq exceeds that head is
    /// ignored and replay starts from `ZERO` (**G4**).
    async fn rehydrate(&mut self, ctx: &Ctx<Host<G>>) -> Result<(), BoxError> {
        let codec = ctx.system().codec();
        // Barrier first (spec §9, invariant G3/G14): wait until the journal's local
        // view reflects every committed write before reading the head, so a grain
        // activating on a freshly-elected leader never rebuilds from a still-
        // draining projection and then serves stale reads. A no-op on Tier 1.
        self.journal.catch_up().await;
        let head = self.journal.head(&self.name).await.map_err(boxed)?;

        let (mut seq, from_snapshot) = match self.journal.load_snapshot(&self.name).await.map_err(boxed)? {
            Some((s_seq, bytes)) if s_seq <= head => {
                self.state = actor_serialization::decode(&*codec, &bytes).map_err(boxed)?;
                self.last_snapshot = s_seq;
                (s_seq, true)
            }
            // No snapshot, or one beyond the committed head (**G4**): the journal
            // is the authority, so replay the whole log from the empty head.
            _ => {
                self.state = G::State::default();
                self.last_snapshot = Seq::ZERO;
                (Seq::ZERO, false)
            }
        };

        let mut replayed = 0u64;
        loop {
            let batch = self.journal.load(&self.name, seq, REPLAY_BATCH).await.map_err(boxed)?;
            if batch.is_empty() {
                break;
            }
            for (s, bytes) in batch {
                let event: G::Event = actor_serialization::decode(&*codec, &bytes).map_err(boxed)?;
                G::apply(&mut self.state, &event);
                seq = s;
                replayed += 1;
            }
        }

        self.head = head;
        self.last_active = ctx.system().now();

        // Run `on_activate` BEFORE announcing the activation: if it fails, the
        // activation aborts (§10) and must leave no `Activated` on the stream — a
        // phantom `Activated` with no matching `Passivated` would both misreport
        // the lifecycle (§13) and corrupt a G6 activation checker.
        let gctx = self.grain_ctx(ctx);
        self.grain.on_activate(&gctx).await?;

        // Now the activation is real: `Rehydrated` describes the rebuild, then
        // `Activated` marks the grain ready to serve its first command (§13).
        let node = ctx.system().node();
        ctx.system().emit_grain_event(GrainEvent::Rehydrated {
            node,
            name: self.name.clone(),
            from_snapshot,
            replayed,
        });
        ctx.system().emit_grain_event(GrainEvent::Activated {
            node,
            name: self.name.clone(),
        });
        self.schedule_idle_check(ctx);
        Ok(())
    }

    /// The §6 per-command protocol, the one place the durability barrier lives.
    pub(crate) async fn run_protocol<M>(
        &mut self,
        msg: M,
        ctx: &Ctx<Host<G>>,
    ) -> Result<M::Reply, GrainError>
    where
        G: GrainHandler<M>,
        M: Message,
    {
        self.last_active = ctx.system().now();

        // 1. Decide: inspect state, produce events + reply. No mutation, no I/O.
        let gctx = self.grain_ctx(ctx);
        let (events, reply) = self.grain.handle(&self.state, msg, &gctx).await;

        // 2. Read path: no events, nothing to commit (§7.5). Serve from the
        //    in-memory activation — a local, replication-free read. This is the
        //    relaxed, **read-your-leader** contract (§7.5): a deposed-but-unfenced
        //    minority leader can serve a stale read until its activation stops.
        //    Writes never fork (Raft fences the commit, §8); only reads can be
        //    stale, and only on the minority side of a partition. Linearizable
        //    reads via a check-quorum leader lease are a deferred upgrade (§16) —
        //    not a per-read consensus round, which would defeat read scaling (§7.8).
        if events.is_empty() {
            return Ok(reply);
        }

        // 3. Encode the batch and append it as one atomic entry (§7.3).
        let codec = ctx.system().codec();
        let mut encoded = Vec::with_capacity(events.len());
        for event in &events {
            match actor_serialization::encode(&*codec, event) {
                Ok(bytes) => encoded.push(bytes),
                Err(e) => return Err(GrainError::Call(CallError::Serialization(e.to_string()))),
            }
        }

        match self.journal.append(&self.name, self.head, encoded).await {
            // 4. Durable on a quorum: fold AFTER durability, advance head, reply.
            AppendOutcome::Committed(new_head) => {
                // The grain is its shard's single writer (§8), so its head MUST
                // advance by exactly this batch. A jump past that means the head we
                // appended from was stale — a projection that lagged at activation,
                // or a prior timed-out append that committed late (§7.2) — so the
                // intervening committed events were never folded into our state. The
                // journal is the authority (G3): step down rather than fold onto a
                // state that is missing them; the next access rehydrates cleanly.
                let expected = Seq::new(self.head.value() + events.len() as u64);
                if new_head != expected {
                    self.forced_step_down(ctx).await;
                    return Err(GrainError::Unavailable("stale head; reactivating".into()));
                }
                for event in &events {
                    G::apply(&mut self.state, event);
                }
                self.head = new_head;
                ctx.system().emit_grain_event(GrainEvent::Committed {
                    node: ctx.system().node(),
                    name: self.name.clone(),
                    seq: new_head.value(),
                });
                self.maybe_snapshot(ctx).await;
                Ok(reply) // OUTPUT GATE releases here.
            }
            // 5. Leadership moved off this node (§8): step down, redirect. The
            //    state is untouched; no fold, no success reply (**G5**).
            AppendOutcome::NotLeader(hint) => {
                self.forced_step_down(ctx).await;
                Err(GrainError::NotLeader(hint))
            }
            // 6. Shard quorum lost or the commit timed out (§11): the append's fate
            //    is ambiguous (it may yet commit, §7.2), so the in-memory head can
            //    no longer be trusted. Step down; the caller retries/fails over and
            //    the next access rehydrates from the journal authority (G3).
            AppendOutcome::Unavailable(why) => {
                self.forced_step_down(ctx).await;
                Err(GrainError::Unavailable(why))
            }
        }
    }

    /// Emit `Passivated` and stop — the one deactivation seam (§13). Emitting
    /// `Passivated` keeps the lifecycle stream balanced (every `Activated` has a
    /// matching `Passivated`/`NodeDown`, the basis of the **G6** singleton checker).
    fn step_down(&self, ctx: &Ctx<Host<G>>) {
        ctx.system().emit_grain_event(GrainEvent::Passivated {
            node: ctx.system().node(),
            name: self.name.clone(),
        });
        ctx.stop();
    }

    /// Deactivate on an involuntary stop — leadership moved (§8), the shard went
    /// unavailable (§11), or the head desynced. Runs `on_passivate` so the grain
    /// can release non-durable activation resources even on a forced step-down (a
    /// layered runtime's per-activation handles — e.g. the agentic harness's
    /// sandbox), then emits `Passivated` and stops. Safe when the journal is
    /// unwritable: `on_passivate` has no journal access (the [`GrainCtx`] exposes
    /// no `persist`). Unlike idle hibernation (§10) it takes **no snapshot**.
    async fn forced_step_down(&mut self, ctx: &Ctx<Host<G>>) {
        let gctx = self.grain_ctx(ctx);
        self.grain.on_passivate(&gctx).await;
        self.step_down(ctx);
    }

    /// Persist a snapshot once enough events have accumulated past the last one
    /// (spec §9). The trigger is configuration, not part of the model.
    async fn maybe_snapshot(&mut self, ctx: &Ctx<Host<G>>) {
        if self.config.snapshot_every == 0 {
            return;
        }
        if self.head.value().saturating_sub(self.last_snapshot.value()) < self.config.snapshot_every {
            return;
        }
        self.snapshot_now(ctx).await;
    }

    /// Persist a snapshot at the current head if any events are uncovered by the
    /// last one (spec §9). A snapshot is only an optimization; an encode or
    /// persist failure is swallowed, leaving the journal as the authority.
    async fn snapshot_now(&mut self, ctx: &Ctx<Host<G>>) {
        if self.head <= self.last_snapshot {
            return;
        }
        let codec = ctx.system().codec();
        let Ok(bytes) = actor_serialization::encode(&*codec, &self.state) else {
            return;
        };
        if let AppendOutcome::Committed(at) =
            self.journal.save_snapshot(&self.name, self.head, bytes).await
        {
            self.last_snapshot = at;
            ctx.system().emit_grain_event(GrainEvent::Snapshotted {
                node: ctx.system().node(),
                name: self.name.clone(),
                at: at.value(),
            });
        }
    }

    /// Hibernate on idle (spec §10): run `on_passivate`, snapshot to bound the
    /// next replay, and stop. The gateway prunes the name via death watch, and
    /// the next message reactivates and rehydrates (invariant **G12**).
    async fn passivate(&mut self, ctx: &Ctx<Host<G>>) {
        let gctx = self.grain_ctx(ctx);
        self.grain.on_passivate(&gctx).await;
        self.snapshot_now(ctx).await;
        self.step_down(ctx); // emit Passivated + stop, the one deactivation seam
    }

    /// Arm the next idle check (spec §10): after `idle_after` of virtual time,
    /// send ourselves a [`CheckIdle`]. A dropped or barely-idle grain reschedules
    /// rather than thrashing.
    fn schedule_idle_check(&self, ctx: &Ctx<Host<G>>) {
        if self.config.idle_after.is_zero() {
            return;
        }
        let me: ActorRef<Host<G>> = ctx.this();
        let sleep = ctx.system().sleep(self.config.idle_after);
        ctx.system().launch(Box::pin(async move {
            sleep.await;
            // Best-effort: if the host already stopped, the tell dead-letters and
            // the chain ends — which is exactly what we want.
            let _ = me.tell(CheckIdle).await;
        }));
    }
}

impl<G: Grain> Actor for Host<G> {
    type System = G::System;

    /// Accept each of the grain's commands over the network as `RunTyped<M>` (spec
    /// §5.5): a caller routed to this node's leader (directly or via the gateway)
    /// asks the host the typed command, and the host runs the §6 protocol. The
    /// list is the grain's own `register` allowlist, bridged through
    /// [`GrainRegistry`] — the framework's dispatch registry is the grain registry.
    fn register(registry: &mut HandlerRegistry<Host<G>>) {
        let mut grain_registry = GrainRegistry::<G>::new();
        G::register(&mut grain_registry);
        for entry in grain_registry.host_entries() {
            entry(registry);
        }
    }

    async fn started(&mut self, ctx: &Ctx<Host<G>>) -> Result<(), BoxError> {
        self.rehydrate(ctx).await
    }
}

impl<G, M> Handler<RunTyped<M>> for Host<G>
where
    G: GrainHandler<M>,
    M: Message,
{
    async fn handle(&mut self, msg: RunTyped<M>, ctx: &Ctx<Host<G>>) -> Result<M::Reply, GrainError> {
        self.run_protocol(msg.0, ctx).await
    }
}

impl<G: Grain> Handler<CheckIdle> for Host<G> {
    async fn handle(&mut self, _msg: CheckIdle, ctx: &Ctx<Host<G>>) {
        let now = ctx.system().now();
        // Idle long enough AND the grain permits eviction (§10): a grain with
        // autonomous, not-yet-journaled work vetoes hibernation until it settles
        // (`can_passivate`), so the host reschedules rather than evicting it
        // mid-flight. Activity arriving after the timer was armed also reschedules.
        if now.duration_since(self.last_active) >= self.config.idle_after
            && self.grain.can_passivate(&self.state)
        {
            self.passivate(ctx).await;
        } else {
            self.schedule_idle_check(ctx);
        }
    }
}

/// Box a local error (journal or codec) as a [`BoxError`] for `Actor::started`.
fn boxed<E: std::error::Error + Send + Sync + 'static>(error: E) -> BoxError {
    Box::new(error)
}
