//! The workflow-facet `Pipeline` grain, shared by `workflow.rs` (scenarios) and
//! `workflow_swarm.rs` (the sweep) — granary §7.17.
//!
//! A self-driving re-entrant workflow: fetch → (sleep) → double → `Finished`, the
//! reference shape a linear DSL would generate. Two things about it are load-
//! bearing for what the suites assert, and both were chosen after the obvious
//! version failed to observe anything.
//!
//! **The effect's value differs on every launch.** `fetch` returns a fresh
//! ordinal drawn from [`Effects`] rather than a constant, and the ordinals are
//! unique across every grain in a run. That is what makes the memo's **write-once**
//! property observable at all: `complete_step` records only a step that is not
//! already done, so the first committed result wins and every later drive resolves
//! from it — but a fixture whose effect always returns `21` cannot tell a memo that
//! was preserved from one that was overwritten with an identical value. This is the
//! property worth asserting, rather than "the effect ran once": [`LaunchGuard`] is
//! per-activation and never journaled, so a re-activation legitimately re-launches
//! an unresolved step and the effect may run many times.
//!
//! **Nothing has to succeed before the workflow commits.** The grain drives from
//! `on_activate`, so *any* touch — including the read that observes it — starts the
//! workflow. An earlier version opened with a `Start` ask, which put a round trip
//! that had to succeed in front of the first commit; under a nemesis that is the
//! difference between a seed that observes the property and one that observes
//! nothing.
//!
//! Generic over the hosting system, like [`granary::AlarmIndex`], so `workflow.rs`
//! drives it on the `Local` tier and `workflow_swarm.rs` drives the same grain on
//! the clustered one.

use std::collections::BTreeMap;
use std::marker::PhantomData;
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::atomic::AtomicBool;
use std::sync::atomic::Ordering;
use std::time::Duration;

use actor_core::Manifest;
use actor_core::Message;
use granary::Alarm;
use granary::Grain;
use granary::GrainCtx;
use granary::GrainHandler;
use granary::GranarySystem;
use granary::LaunchGuard;
use granary::StepDone;
use granary::StepId;
use granary::Workflow;
use granary::complete_step;
use serde::Deserialize;
use serde::Serialize;

/// The grain type name both suites host under.
pub const PIPELINE_TYPE: &str = "test.Pipeline";

// Step ids — the workflow's stable call-site ordinals.
/// An external effect, whose result is memoized. The step the write-once check reads.
pub const STEP_FETCH: StepId = 0;
/// The sleep gate, recorded by `on_alarm`. Only present when [`PipelineConfig::sleep`] is.
pub const STEP_WOKE: StepId = 1;
/// A second external effect over the fetched value.
pub const STEP_DOUBLE: StepId = 2;

/// What the effects of one *run* did: the launch ordinals handed out, and how many
/// times each grain launched each step.
///
/// Reset at the start of every run ([`Effects::reset`]) rather than accumulated
/// across a sweep, for two reasons. The ordinals must be a function of the seed
/// alone, or a reproducibility replay would hand the same run different step values
/// than its first pass; and "did this grain re-launch" is a claim about one run,
/// which is exactly the distinction `docs/simulation-testing.md` draws between a
/// sweep-scoped tally and a run-scoped expectation.
#[derive(Default)]
struct Runs {
    /// The next launch ordinal. Every launched effect in a run draws a distinct
    /// one, so an overwritten memo carries a value no other step could have put
    /// there.
    next: u32,
    /// `(grain key, step) → launches this run`.
    launched: BTreeMap<(String, StepId), u32>,
}

/// The effect-side handle a [`Pipeline`] activation is built with. Shared into the
/// grain through the `granary_named` factory, so it survives re-activation and, in
/// a cluster, spans every node the grain may be hosted on.
#[derive(Clone, Default)]
pub struct Effects {
    runs: Arc<Mutex<Runs>>,
    /// Whether `fetch`'s next launch should fail (the retry scenario). Consumed by
    /// the launch that sees it.
    fail_next_fetch: Arc<AtomicBool>,
}

impl Effects {
    /// Forget the previous run's ordinals and launch counts. A workload calls this
    /// at the top of `drive`, before any grain can be touched.
    pub fn reset(&self) {
        let mut runs = self.runs.lock().expect("effects mutex poisoned");
        runs.next = 0;
        runs.launched.clear();
    }

    /// Record a launch of `step` by grain `key` and return the value its effect
    /// should produce — a fresh ordinal, distinct from every other launch this run.
    fn launch(&self, key: &str, step: StepId) -> u32 {
        let mut runs = self.runs.lock().expect("effects mutex poisoned");
        runs.next += 1;
        *runs.launched.entry((key.to_string(), step)).or_default() += 1;
        runs.next
    }

    /// How many times grain `key` launched `step`'s effect this run. More than one
    /// is a re-launch: the activation that owned the first launch went away before
    /// the step resolved, which is the interruption the write-once check needs.
    pub fn launches(&self, key: &str, step: StepId) -> u32 {
        self.runs
            .lock()
            .expect("effects mutex poisoned")
            .launched
            .get(&(key.to_string(), step))
            .copied()
            .unwrap_or(0)
    }

    /// Every grain key that launched `step` more than once this run.
    pub fn relaunched(&self, step: StepId) -> Vec<String> {
        self.runs
            .lock()
            .expect("effects mutex poisoned")
            .launched
            .iter()
            .filter(|((_, s), n)| *s == step && **n > 1)
            .map(|((key, _), _)| key.clone())
            .collect()
    }

    /// Arm the next `fetch` launch to fail without recording a result — the
    /// alarm-free core of a `retry` step.
    pub fn fail_next_fetch(&self) {
        self.fail_next_fetch.store(true, Ordering::SeqCst);
    }
}

/// How one hosting of the pipeline is shaped.
#[derive(Clone, Copy)]
pub struct PipelineConfig {
    /// The durable sleep between the two effects, or `None` for the shorter shape:
    /// fetch → double, with no alarm at all.
    ///
    /// The sweep takes `None`. A `sleep` needs a hosted `AlarmIndex` and its
    /// per-shard driver, which never quiesces — a real cost in checkers dropped
    /// (`alarm_swarm.rs` carries that argument) and one that buys the memo check
    /// nothing, since the property is decided by `fetch`'s *first* commit and
    /// everything after it is a longer chain to the same claim.
    pub sleep: Option<Duration>,
    /// Whether the grain may hibernate with a step still in flight.
    ///
    /// `true` is half of what makes a re-launch something the *workload* drives
    /// rather than something it waits for the nemesis to produce; the other half is
    /// [`effect_latency`](PipelineConfig::effect_latency). Together they open a
    /// window in which the activation that launched `fetch` hibernates while its
    /// effect is still outstanding, so the next touch re-activates the grain, finds
    /// the step unresolved, and launches it again — with a different value.
    /// Write-once is what then decides which one the memo keeps.
    pub hibernate_mid_workflow: bool,
    /// How long an effect takes before it self-`tell`s its result back, or `None`
    /// for one that resolves within the same burst as its launch.
    ///
    /// An effect that returns immediately never leaves a window to be interrupted
    /// in: the `StepDone` is already in the mailbox when the re-activation happens,
    /// so it is handled before the drive that would re-launch, and the second
    /// launch never occurs. That is the shape an earlier fixture had, and it is why
    /// its re-launches only ever came from the nemesis — on the seeds calm enough
    /// to observe a memo, never.
    pub effect_latency: Option<Duration>,
}

impl Default for PipelineConfig {
    fn default() -> Self {
        PipelineConfig {
            sleep: Some(Duration::from_millis(50)),
            hibernate_mid_workflow: false,
            effect_latency: None,
        }
    }
}

/// A self-driving workflow grain: fetch → sleep → double → `Finished`, with the
/// sleep present only when [`PipelineConfig::sleep`] is.
pub struct Pipeline<S> {
    fx: Effects,
    cfg: PipelineConfig,
    eph: Mutex<Ephemeral>,
    _system: PhantomData<fn() -> S>,
}

impl<S> Pipeline<S> {
    /// The factory a host is built with: one `Effects` and one shape, cloned into
    /// every activation.
    pub fn factory(fx: Effects, cfg: PipelineConfig) -> Arc<dyn Fn() -> Pipeline<S> + Send + Sync> {
        Arc::new(move || Pipeline {
            fx: fx.clone(),
            cfg,
            eph: Mutex::new(Ephemeral::default()),
            _system: PhantomData,
        })
    }
}

#[derive(Default)]
struct Ephemeral {
    guard: LaunchGuard,
}

#[derive(Default, Serialize, Deserialize)]
pub struct PipelineState {
    /// The doubled value, once the workflow ran to completion.
    pub finished: Option<u32>,
}

#[derive(Serialize, Deserialize)]
pub enum PipelineEvent {
    Finished(u32),
}

impl<S: GranarySystem> Pipeline<S> {
    fn schedule_drive(&self, ctx: &GrainCtx<Self>) {
        let this = ctx.this();
        ctx.system().launch(Box::pin(async move {
            let _ = this.tell(Drive).await;
        }));
    }

    /// The re-entrant workflow body (spec §7.17): re-run after every commit, it
    /// resolves completed steps from the memo and drives the first incomplete one.
    fn drive(&self, state: &PipelineState, ctx: &GrainCtx<Self>) -> Vec<PipelineEvent> {
        if state.finished.is_some() {
            return Vec::new();
        }
        let wf = ctx.workflow();

        // Step FETCH: an external effect, launched once per activation, its result
        // memoized. Its value is a fresh ordinal, so a memo that was overwritten
        // reads differently from one that was kept.
        let fetched: Option<u32> = wf.result(STEP_FETCH).expect("decode");
        let Some(fetched) = fetched else {
            self.launch_fetch(ctx);
            return Vec::new();
        };

        // Sleep: gate on the WOKE step, set by on_alarm. Arm the alarm once.
        if let Some(after) = self.cfg.sleep
            && !wf.is_done(STEP_WOKE)
        {
            if ctx.alarm().pending().is_none() {
                ctx.alarm().set_after(after);
            }
            return Vec::new();
        }

        // Step DOUBLE: a second effect over the fetched value, memoized. Its value
        // is derived rather than fresh, which makes the terminal state a second
        // witness: `finished` can only ever be twice the memo FETCH settled on.
        let doubled: Option<u32> = wf.result(STEP_DOUBLE).expect("decode");
        let Some(doubled) = doubled else {
            self.launch_double(ctx, fetched);
            return Vec::new();
        };

        vec![PipelineEvent::Finished(doubled)]
    }

    fn launch_fetch(&self, ctx: &GrainCtx<Self>) {
        if !self.eph.lock().unwrap().guard.claim(STEP_FETCH) {
            return; // already in flight this activation
        }
        // A failing launch (the retry shape) records no result and asks the grain
        // to forget its claim, so the next drive re-launches it.
        if self.fx.fail_next_fetch.swap(false, Ordering::SeqCst) {
            let this = ctx.this();
            ctx.system().launch(Box::pin(async move {
                let _ = this.tell(Retry { id: STEP_FETCH }).await;
            }));
            return;
        }
        let value = self.fx.launch(ctx.name().key(), STEP_FETCH);
        self.run_effect(ctx, StepDone::new(STEP_FETCH, &value));
    }

    fn launch_double(&self, ctx: &GrainCtx<Self>, value: u32) {
        if !self.eph.lock().unwrap().guard.claim(STEP_DOUBLE) {
            return;
        }
        self.fx.launch(ctx.name().key(), STEP_DOUBLE);
        self.run_effect(ctx, StepDone::new(STEP_DOUBLE, &(value * 2)));
    }

    /// The off-command-path half of a step (§7.17): take `latency`, then self-`tell`
    /// the result back. A `tell` that the faults refuse is simply a step that did
    /// not resolve — the next drive re-launches it.
    fn run_effect(&self, ctx: &GrainCtx<Self>, done: StepDone) {
        let this = ctx.this();
        let system = ctx.system().clone();
        let latency = self.cfg.effect_latency;
        ctx.system().launch(Box::pin(async move {
            if let Some(latency) = latency {
                system.sleep(latency).await;
            }
            let _ = this.tell(done).await;
        }));
    }
}

impl<S: GranarySystem> Grain for Pipeline<S> {
    type System = S;
    type State = PipelineState;
    type Event = PipelineEvent;
    type Facets = (Workflow, Alarm);
    const GRAIN_TYPE: &'static str = PIPELINE_TYPE;

    fn apply(state: &mut PipelineState, event: &PipelineEvent) {
        match event {
            PipelineEvent::Finished(v) => state.finished = Some(*v),
        }
    }

    fn register(r: &mut granary::GrainRegistry<Self>) {
        r.accept::<Drive>();
        r.accept::<StepDone>();
        r.accept::<Retry>();
        r.accept::<Read>();
        r.accept::<ReadMemo>();
    }

    async fn on_activate(&mut self, ctx: &GrainCtx<Self>) -> Result<(), actor_core::BoxError> {
        self.eph.lock().unwrap().guard.reset();
        // Resume an in-flight workflow after a (re)activation — and start one on the
        // first, so the workflow needs no message of its own to begin.
        self.schedule_drive(ctx);
        Ok(())
    }

    // The sleep fires here with no caller: record the WOKE gate and re-drive.
    async fn on_alarm(&self, _state: &PipelineState, ctx: &GrainCtx<Self>) -> Vec<PipelineEvent> {
        ctx.workflow().record(STEP_WOKE, &());
        self.schedule_drive(ctx);
        Vec::new()
    }

    fn can_passivate(&self, state: &PipelineState) -> bool {
        // Either the workflow is over, or this hosting has opted into hibernating
        // under an in-flight step (see `PipelineConfig::hibernate_mid_workflow`).
        self.cfg.hibernate_mid_workflow || state.finished.is_some()
    }
}

/// Re-run the workflow body. Self-`tell`ed after every activation and every commit.
#[derive(Clone, Serialize, Deserialize)]
pub struct Drive;
impl Message for Drive {
    type Reply = ();
    const MANIFEST: Manifest = Manifest::new("test.pipeline.Drive");
}
impl<S: GranarySystem> GrainHandler<Drive> for Pipeline<S> {
    async fn handle(
        &self,
        state: &PipelineState,
        _m: Drive,
        ctx: &GrainCtx<Self>,
    ) -> (Vec<PipelineEvent>, ()) {
        (self.drive(state, ctx), ())
    }
}

impl<S: GranarySystem> GrainHandler<StepDone> for Pipeline<S> {
    async fn handle(
        &self,
        _s: &PipelineState,
        msg: StepDone,
        ctx: &GrainCtx<Self>,
    ) -> (Vec<PipelineEvent>, ()) {
        // `complete_step` is the write-once gate: a step already recorded keeps the
        // result it has, and this second `StepDone` commits nothing.
        let events = complete_step(ctx, msg);
        self.schedule_drive(ctx); // a committed step result unblocks the next drive
        (events, ())
    }
}

/// A step's effect failed: forget the activation's launch claims so the next drive
/// re-launches it (the alarm-free core of a `retry`; a real backoff arms an alarm
/// first).
#[derive(Clone, Serialize, Deserialize)]
pub struct Retry {
    pub id: StepId,
}
impl Message for Retry {
    type Reply = ();
    const MANIFEST: Manifest = Manifest::new("test.pipeline.Retry");
}
impl<S: GranarySystem> GrainHandler<Retry> for Pipeline<S> {
    async fn handle(
        &self,
        _s: &PipelineState,
        _msg: Retry,
        ctx: &GrainCtx<Self>,
    ) -> (Vec<PipelineEvent>, ()) {
        // Only the failed step can be in flight here, so a full reset is a targeted
        // un-claim: the next drive re-launches it.
        self.eph.lock().unwrap().guard.reset();
        self.schedule_drive(ctx);
        (vec![], ())
    }
}

/// The workflow's terminal value, or `None` while it is still running.
#[derive(Clone, Serialize, Deserialize)]
pub struct Read;
impl Message for Read {
    type Reply = Option<u32>;
    const MANIFEST: Manifest = Manifest::new("test.pipeline.Read");
}
impl<S: GranarySystem> GrainHandler<Read> for Pipeline<S> {
    async fn handle(
        &self,
        state: &PipelineState,
        _m: Read,
        _ctx: &GrainCtx<Self>,
    ) -> (Vec<PipelineEvent>, Option<u32>) {
        (vec![], state.finished)
    }
}

/// What the memo holds for [`STEP_FETCH`] — `None` until the step's first result
/// commits. The observation the write-once check is made of: two reads that both
/// answer `Some` must answer the same `Some`.
#[derive(Clone, Serialize, Deserialize)]
pub struct ReadMemo;
impl Message for ReadMemo {
    type Reply = Option<u32>;
    const MANIFEST: Manifest = Manifest::new("test.pipeline.ReadMemo");
}
impl<S: GranarySystem> GrainHandler<ReadMemo> for Pipeline<S> {
    async fn handle(
        &self,
        _s: &PipelineState,
        _m: ReadMemo,
        ctx: &GrainCtx<Self>,
    ) -> (Vec<PipelineEvent>, Option<u32>) {
        let memo: Option<u32> = ctx.workflow().result(STEP_FETCH).expect("decode");
        (vec![], memo)
    }
}
