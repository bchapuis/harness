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
//! the single-node `Local` journal and the clustered `Quorum` journal unchanged.

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

use crate::blobs::GrainBlobs;
use crate::config::GranaryConfig;
use crate::error::GrainError;
use crate::event::GrainEvent;
use crate::facet::CompositeSnapshot;
use crate::facet::EVENT_TAG;
use crate::facet::FacetCell;
use crate::facet::FacetEnv;
use crate::facet::FacetSet;
use crate::facet::split_record;
use crate::facet::tag_record;
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
use crate::subscription::RecordBatch;
use crate::subscription::SUB_BUFFER;
use crate::subscription::Subscribe;
use crate::subscription::Subscribed;
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
    /// Live record sinks (spec §7.9): one bounded channel per subscription, drained
    /// by a forwarder task that pushes to the subscriber's sink. Ephemeral
    /// activation state (§1, §10) — dropped when the host stops, so a move or
    /// hibernation ends every subscription and subscribers re-subscribe (**G3**).
    sinks: Vec<async_channel::Sender<Arc<RecordBatch>>>,
    /// The facet cell (spec §7.12): the declared facets' committed forms and the
    /// per-command stage, shared with the [`GrainCtx`] accessors. Rebuilt from the
    /// composite snapshot plus tagged records on rehydration (**G3**), exactly as
    /// `state` is.
    facets: Arc<FacetCell<G::Facets>>,
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
            sinks: Vec::new(),
            facets: Arc::new(FacetCell::new()),
        }
    }

    /// Build the handler/lifecycle context for the grain (§4.3).
    fn grain_ctx(&self, ctx: &Ctx<Host<G>>) -> GrainCtx<G> {
        GrainCtx::new(
            self.grain_type,
            self.name.clone(),
            ctx.system().clone(),
            self.gateway.clone(),
            Arc::clone(&self.journal),
            Arc::clone(&self.facets),
        )
    }

    /// The facet environment (spec §7.12): a bare blob handle (no facet-root
    /// union — the host retains roots explicitly where it sweeps) and the
    /// node-local scratch directory a physical facet materializes under (§7.14).
    fn facet_env(&self) -> FacetEnv {
        FacetEnv::new(
            GrainBlobs::new(Arc::clone(&self.journal), self.name.clone()),
            self.config.scratch_dir(),
        )
    }

    /// Rebuild state from the journal (spec §9): load the latest snapshot, then
    /// replay the events after it, folding with `apply`. The head is taken from
    /// the journal's authority (**G3**); a snapshot whose seq exceeds that head is
    /// ignored and replay starts from `ZERO` (**G4**).
    async fn rehydrate(&mut self, ctx: &Ctx<Host<G>>) -> Result<(), BoxError> {
        let codec = ctx.system().codec();
        // `head` is the rehydration barrier (spec §8, §9, invariant G3/G14): on the
        // `Quorum` tier it recovers the grain's head from a write quorum by
        // read-repair, so a grain activating on a freshly-elected leader never
        // rebuilds from a stale head and then serves stale reads; it fails fast with
        // `Unavailable` while the shard is still electing (§8.3), aborting the
        // activation so the caller retries. A local read on the `Local` tier.
        let head = self.journal.head(&self.name).await.map_err(boxed)?;

        let (mut seq, from_snapshot) = match self
            .journal
            .load_snapshot(&self.name)
            .await
            .map_err(boxed)?
        {
            Some((s_seq, bytes)) if s_seq <= head => {
                // The snapshot is a composite (spec §7.12): facet 0's `State`
                // plus one contribution per declared facet, all at `s_seq`. G4
                // applies to it as a whole; a part that will not restore aborts
                // the activation rather than serving a half-rebuilt grain.
                let composite = CompositeSnapshot::decode(&bytes).map_err(boxed)?;
                self.state =
                    actor_serialization::decode(&*codec, &composite.state).map_err(boxed)?;
                let forms = G::Facets::restore(&composite.facets, &self.facet_env())
                    .await
                    .map_err(boxed)?;
                self.facets.install(forms);
                self.last_snapshot = s_seq;
                (s_seq, true)
            }
            // No snapshot, or one beyond the committed head (**G4**): the journal
            // is the authority, so replay the whole log from the empty head.
            // Restore runs with no contributions all the same — a physical facet
            // materializes its empty form here (§7.14).
            _ => {
                self.state = G::State::default();
                let forms = G::Facets::restore(&[], &self.facet_env())
                    .await
                    .map_err(boxed)?;
                self.facets.install(forms);
                self.last_snapshot = Seq::ZERO;
                (Seq::ZERO, false)
            }
        };

        let mut replayed = 0u64;
        loop {
            let batch = self
                .journal
                .load(&self.name, seq, REPLAY_BATCH)
                .await
                .map_err(boxed)?;
            if batch.is_empty() {
                break;
            }
            for (s, bytes) in batch {
                // Dispatch each record by its facet tag (spec §7.12). A tag no
                // declared facet claims aborts the activation (**G19**): the
                // grain's history must never be silently misread by a runtime
                // missing one of its facets.
                let (tag, payload) = split_record(&bytes).map_err(boxed)?;
                if tag == EVENT_TAG {
                    let event: G::Event =
                        actor_serialization::decode(&*codec, payload).map_err(boxed)?;
                    G::apply(&mut self.state, &event);
                } else {
                    self.facets.fold_replay(tag, payload).map_err(boxed)?;
                }
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

        // 1. Decide: inspect state, produce events + reply. The handler stages
        //    facet operations through the ctx accessors (spec §7.12): a logical
        //    facet's overlay, a physical facet's local transaction — neither
        //    observable before the commit (§4.2).
        if let Err(e) = self.facets.begin() {
            // A physical facet could not open its per-command work: its
            // materialization can no longer be trusted (G20).
            self.forced_step_down(ctx).await;
            return Err(GrainError::Unavailable(format!("facet begin: {e}")));
        }
        let gctx = self.grain_ctx(ctx);
        let (events, reply) = self.grain.handle(&self.state, msg, &gctx).await;

        // 2. Encode the events (facet 0) under their record tag (§7.12). Encoded
        //    BEFORE the facet seal, so a serialization failure abandons the
        //    stage with no physical local commit to unwind.
        let codec = ctx.system().codec();
        let mut encoded = Vec::with_capacity(events.len());
        for event in &events {
            match actor_serialization::encode(&*codec, event) {
                Ok(bytes) => encoded.push(tag_record(EVENT_TAG, &bytes)),
                Err(e) => {
                    self.facets.abandon();
                    return Err(GrainError::Call(CallError::Serialization(e.to_string())));
                }
            }
        }

        // 3. Seal and drain the facet stages into tagged records (§7.12): a
        //    physical facet commits its local transaction and captures the delta
        //    here (§7.14). All records join one atomic batch (**G19**).
        let facet_records = match self.facets.seal_and_drain() {
            Ok(records) => records,
            Err(e) => {
                // A physical local commit failed: the materialization is tainted;
                // discard it and step down (G20). The next activation rebuilds.
                self.forced_step_down(ctx).await;
                return Err(GrainError::Unavailable(format!("facet seal: {e}")));
            }
        };
        for (tag, payload) in &facet_records {
            encoded.push(tag_record(*tag, payload));
        }

        // 4. Read path: an empty batch after the drain commits nothing (§7.5).
        //    Serve from the in-memory activation — a local, replication-free
        //    read. This is the relaxed, **read-your-leader** contract (§7.5): a
        //    deposed-but-unfenced minority leader can serve a stale read until
        //    its activation stops. Writes never fork (Raft fences the commit,
        //    §8); only reads can be stale, and only on the minority side of a
        //    partition. Linearizable reads via a check-quorum leader lease are a
        //    deferred upgrade (§16) — not a per-read consensus round, which
        //    would defeat read scaling (§7.8).
        if encoded.is_empty() {
            return Ok(reply);
        }
        let batch_len = encoded.len() as u64;

        // Keep the encoded bytes for subscription delivery (§7.9) only when a
        // sink is attached, so an unsubscribed grain pays nothing; `append`
        // consumes the original.
        let from = self.head;
        let to_deliver = (!self.sinks.is_empty()).then(|| encoded.clone());

        match self.journal.append(&self.name, self.head, encoded).await {
            // 5. Durable on a quorum: fold AFTER durability, advance head, reply.
            AppendOutcome::Committed(new_head) => {
                // The grain is its shard's single writer (§8), so its head MUST
                // advance by exactly this batch. A jump past that means the head we
                // appended from was stale — a projection that lagged at activation,
                // or a prior timed-out append that committed late (§7.2) — so the
                // intervening committed events were never folded into our state. The
                // journal is the authority (G3): step down rather than fold onto a
                // state that is missing them; the next access rehydrates cleanly.
                let expected = Seq::new(self.head.value() + batch_len);
                if new_head != expected {
                    self.forced_step_down(ctx).await;
                    return Err(GrainError::Unavailable("stale head; reactivating".into()));
                }
                for event in &events {
                    G::apply(&mut self.state, event);
                }
                // Fold the facets' records on the live path (§7.12): logical
                // facets fold exactly as replay will (F1); a physical facet's
                // form already mutated at its local commit and is skipped. A
                // facet that cannot fold its own just-drained record is a bug;
                // the in-memory forms can no longer be trusted, so step down and
                // let rehydration rebuild from the journal authority (G3).
                for (tag, payload) in &facet_records {
                    if let Err(e) = self.facets.fold_live(*tag, payload) {
                        self.forced_step_down(ctx).await;
                        return Err(GrainError::Unavailable(format!("facet fold: {e}")));
                    }
                }
                self.head = new_head;
                ctx.system().emit_grain_event(GrainEvent::Committed {
                    node: ctx.system().node(),
                    name: self.name.clone(),
                    seq: new_head.value(),
                });
                // Push the batch to subscribers (§7.9), at the same point as the
                // `Committed` event and after the output gate — observational, so
                // it never gates the commit (**G5**).
                if let Some(bytes) = to_deliver {
                    self.deliver_records(from, new_head, bytes);
                }
                self.maybe_snapshot(ctx).await;
                Ok(reply) // OUTPUT GATE releases here.
            }
            // 6. Leadership moved off this node (§8): step down, redirect. The
            //    state is untouched; no fold, no success reply (**G5**). Physical
            //    facet materializations mutated at their local commit and are
            //    discarded (**G20**, §7.14); the next activation rebuilds them
            //    from the composite snapshot plus committed records.
            AppendOutcome::NotLeader(hint) => {
                self.forced_step_down(ctx).await;
                Err(GrainError::NotLeader(hint))
            }
            // 7. Shard quorum lost or the commit timed out (§11): the append's fate
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
    /// unavailable (§11), or the head desynced. The in-memory forms can no longer
    /// be trusted, so every physical facet materialization is discarded (**G20**,
    /// §7.14); the next activation rebuilds from the composite snapshot plus
    /// committed records. Runs `on_passivate` so the grain can release
    /// non-durable activation resources even on a forced step-down (a layered
    /// runtime's per-activation handles — e.g. the agentic harness's sandbox),
    /// then emits `Passivated` and stops. Safe when the journal is unwritable:
    /// `on_passivate` has no journal access (the [`GrainCtx`] exposes no
    /// `persist`). Unlike idle hibernation (§10) it takes **no snapshot**.
    async fn forced_step_down(&mut self, ctx: &Ctx<Host<G>>) {
        self.facets.discard();
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
        if self.head.value().saturating_sub(self.last_snapshot.value()) < self.config.snapshot_every
        {
            return;
        }
        self.snapshot_now(ctx).await;
    }

    /// Persist a snapshot at the current head if any events are uncovered by the
    /// last one (spec §9). A snapshot is only an optimization; an encode or
    /// persist failure is swallowed, leaving the journal as the authority.
    ///
    /// The payload is the **composite** (spec §7.12): facet 0's `State` plus one
    /// contribution per declared facet, all at this head. Facet contributions
    /// run against a forms *clone*, so no lock spans the (possibly blob-putting,
    /// §7.14) await.
    async fn snapshot_now(&mut self, ctx: &Ctx<Host<G>>) {
        if self.head <= self.last_snapshot {
            return;
        }
        let codec = ctx.system().codec();
        let Ok(state) = actor_serialization::encode(&*codec, &self.state) else {
            return;
        };
        let forms = self.facets.forms();
        let Ok(facets) = G::Facets::snapshot(&forms, &self.facet_env()).await else {
            return;
        };
        let Ok(bytes) = (CompositeSnapshot { state, facets }).encode() else {
            return;
        };
        if let AppendOutcome::Committed(at) = self
            .journal
            .save_snapshot(&self.name, self.head, bytes)
            .await
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

    /// Push one committed batch to every live sink (spec §7.9). `try_send`, never
    /// `send`: a sink whose buffer is full is dropped — its forwarder ends when
    /// the channel closes, the subscriber re-subscribes and backfills by `load` —
    /// so a slow subscriber never blocks a write. This only enqueues to
    /// in-process channels; the cross-node push happens in each forwarder.
    fn deliver_records(&mut self, from: Seq, to: Seq, bytes: Vec<Vec<u8>>) {
        let records: Vec<(u64, Vec<u8>)> = (from.value() + 1..=to.value()).zip(bytes).collect();
        let batch = Arc::new(RecordBatch {
            from: from.value(),
            records,
        });
        self.sinks
            .retain(|sink| sink.try_send(batch.clone()).is_ok());
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
        // The framework built-in: every grain type's host accepts `Subscribe`
        // (spec §7.9), the read-path analogue of `head`/`load`, independent of
        // the grain's own command allowlist below.
        registry.accept::<Subscribe<G>>();
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
    async fn handle(
        &mut self,
        msg: RunTyped<M>,
        ctx: &Ctx<Host<G>>,
    ) -> Result<M::Reply, GrainError> {
        self.run_protocol(msg.0, ctx).await
    }
}

impl<G: Grain> Handler<Subscribe<G>> for Host<G> {
    /// Register a record sink (spec §7.9). Spawns a forwarder that drains a
    /// bounded channel and pushes batches to the subscriber's sink off the host's
    /// executor — so transport backpressure never reaches the write path — and
    /// returns the committed head so the subscriber knows how far to backfill.
    async fn handle(&mut self, msg: Subscribe<G>, ctx: &Ctx<Host<G>>) -> Subscribed {
        let (tx, rx) = async_channel::bounded::<Arc<RecordBatch>>(SUB_BUFFER);
        let sink = msg.sink;
        ctx.system().launch(Box::pin(async move {
            // Ends when the host drops `tx` (deactivation, §10) or the sink is
            // gone; either way the subscriber re-subscribes and backfills (§7.9).
            while let Ok(batch) = rx.recv().await {
                if sink.tell((*batch).clone()).await.is_err() {
                    break;
                }
            }
        }));
        self.sinks.push(tx);
        Subscribed { head: self.head }
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
