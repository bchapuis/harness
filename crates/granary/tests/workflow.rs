//! Durable-workflow tests under deterministic simulation (granary §16).
//!
//! Exercises the [`Workflow`] step memo + the [`Alarm`]-backed `sleep` through a
//! self-driving pipeline grain — the reference shape a linear DSL would generate.
//! Proves the property that matters: a step's effect runs **at most once** across a
//! mid-workflow passivation (memoization), `sleep` resumes the workflow with no
//! caller (the alarm), and a `retry` step re-launches after an alarm-backed backoff.

use std::sync::Arc;
use std::sync::Mutex;
use std::sync::atomic::AtomicU32;
use std::sync::atomic::Ordering;
use std::time::Duration;

use actor_core::EventSink;
use actor_core::LocalSystemBuilder;
use actor_core::Manifest;
use actor_core::Message;
use actor_simulation::Recorder;
use actor_simulation::SimSystem;
use actor_simulation::Simulation;
use granary::Alarm;
use granary::Grain;
use granary::GrainCtx;
use granary::GrainEvent;
use granary::GrainHandler;
use granary::GrainRef;
use granary::GranaryConfig;
use granary::GranaryExt;
use granary::GranarySystem;
use granary::LaunchGuard;
use granary::StepDone;
use granary::Workflow;
use granary::complete_step;
use serde::Deserialize;
use serde::Serialize;

// Step ids — the workflow's stable call-site ordinals.
const STEP_FETCH: u32 = 0; // an external effect, run once
const STEP_WOKE: u32 = 1; // the sleep gate (recorded by on_alarm)
const STEP_DOUBLE: u32 = 2; // a second external effect

/// How many times each grain's fetch/double effect actually ran — the at-most-once
/// witness. Shared into the grain via the `granary_named` factory, so it survives
/// re-activation (the factory captures one `Arc` and clones the handle per build).
#[derive(Clone, Default)]
struct Effects {
    fetch_runs: Arc<AtomicU32>,
    double_runs: Arc<AtomicU32>,
    // Whether `fetch` should fail on its first launch this process (retry test).
    fail_first_fetch: Arc<AtomicU32>,
}

/// A three-stage workflow: fetch → sleep(50ms) → double → Finished.
struct Pipeline {
    fx: Effects,
    eph: Mutex<Ephemeral>,
}

#[derive(Default)]
struct Ephemeral {
    this: Option<GrainRef<Pipeline>>,
    guard: LaunchGuard,
}

#[derive(Default, Serialize, Deserialize)]
struct PipelineState {
    finished: Option<u32>,
}

#[derive(Serialize, Deserialize)]
enum PipelineEvent {
    Finished(u32),
}

impl Pipeline {
    fn schedule_drive(&self, ctx: &GrainCtx<Self>) {
        let this = ctx.this();
        ctx.system().launch(Box::pin(async move {
            let _ = this.tell(Drive).await;
        }));
    }

    /// The re-entrant workflow body (spec §16): re-run after every commit, it
    /// resolves completed steps from the memo and drives the first incomplete one.
    fn drive(&self, state: &PipelineState, ctx: &GrainCtx<Self>) -> Vec<PipelineEvent> {
        if state.finished.is_some() {
            return Vec::new();
        }
        let wf = ctx.workflow();

        // Step FETCH: an external effect, launched once, its result memoized.
        let fetched: Option<u32> = wf.result(STEP_FETCH).expect("decode");
        let Some(fetched) = fetched else {
            self.launch_fetch(ctx);
            return Vec::new();
        };

        // Sleep: gate on the WOKE step, set by on_alarm. Arm the alarm once.
        if !wf.is_done(STEP_WOKE) {
            if ctx.alarm().pending().is_none() {
                ctx.alarm().set_after(Duration::from_millis(50));
            }
            return Vec::new();
        }

        // Step DOUBLE: a second effect over the fetched value, memoized.
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
        let this = ctx.this();
        let runs = self.fx.fetch_runs.clone();
        let fail_first = self.fx.fail_first_fetch.clone();
        ctx.system().launch(Box::pin(async move {
            // A failing first attempt (retry test): record no result and ask the
            // grain to re-launch, the alarm-free shape of a `retry` step.
            if fail_first.swap(0, Ordering::SeqCst) == 1 {
                let _ = this.tell(Retry { id: STEP_FETCH }).await;
                return;
            }
            runs.fetch_add(1, Ordering::SeqCst);
            let _ = this.tell(StepDone::new(STEP_FETCH, &21u32)).await;
        }));
    }

    fn launch_double(&self, ctx: &GrainCtx<Self>, value: u32) {
        if !self.eph.lock().unwrap().guard.claim(STEP_DOUBLE) {
            return;
        }
        let this = ctx.this();
        let runs = self.fx.double_runs.clone();
        ctx.system().launch(Box::pin(async move {
            runs.fetch_add(1, Ordering::SeqCst);
            let _ = this.tell(StepDone::new(STEP_DOUBLE, &(value * 2))).await;
        }));
    }
}

impl Grain for Pipeline {
    type System = SimSystem;
    type State = PipelineState;
    type Event = PipelineEvent;
    type Facets = (Workflow, Alarm);
    const GRAIN_TYPE: &'static str = "test.Pipeline";

    fn apply(state: &mut PipelineState, event: &PipelineEvent) {
        match event {
            PipelineEvent::Finished(v) => state.finished = Some(*v),
        }
    }

    fn register(r: &mut granary::GrainRegistry<Self>) {
        r.accept::<Start>();
        r.accept::<Drive>();
        r.accept::<StepDone>();
        r.accept::<Retry>();
        r.accept::<Read>();
    }

    async fn on_activate(&mut self, ctx: &GrainCtx<Self>) -> Result<(), actor_core::BoxError> {
        let mut eph = self.eph.lock().unwrap();
        eph.this = Some(ctx.this());
        eph.guard.reset();
        drop(eph);
        // Resume an in-flight workflow after a (re)activation.
        self.schedule_drive(ctx);
        Ok(())
    }

    // The sleep fires here with no caller: record the WOKE gate and re-drive.
    async fn on_alarm(&self, _state: &PipelineState, ctx: &GrainCtx<Self>) -> Vec<PipelineEvent> {
        ctx.workflow().record(STEP_WOKE, &());
        self.schedule_drive(ctx);
        Vec::new()
    }

    // Do not hibernate mid-workflow (the alarm veto covers the sleep; this covers
    // the launched-effect windows).
    fn can_passivate(&self, state: &PipelineState) -> bool {
        state.finished.is_some()
    }
}

#[derive(Clone, Serialize, Deserialize)]
struct Start;
impl Message for Start {
    type Reply = ();
    const MANIFEST: Manifest = Manifest::new("test.Start");
}
impl GrainHandler<Start> for Pipeline {
    async fn handle(
        &self,
        _s: &PipelineState,
        _m: Start,
        ctx: &GrainCtx<Self>,
    ) -> (Vec<PipelineEvent>, ()) {
        self.schedule_drive(ctx);
        (vec![], ())
    }
}

#[derive(Clone, Serialize, Deserialize)]
struct Drive;
impl Message for Drive {
    type Reply = ();
    const MANIFEST: Manifest = Manifest::new("test.Drive");
}
impl GrainHandler<Drive> for Pipeline {
    async fn handle(
        &self,
        state: &PipelineState,
        _m: Drive,
        ctx: &GrainCtx<Self>,
    ) -> (Vec<PipelineEvent>, ()) {
        let events = self.drive(state, ctx);
        // If the workflow made progress that unblocks the next step, re-drive.
        if !events.is_empty() {
            // terminal: nothing more to do
        }
        (events, ())
    }
}

impl GrainHandler<StepDone> for Pipeline {
    async fn handle(
        &self,
        _s: &PipelineState,
        msg: StepDone,
        ctx: &GrainCtx<Self>,
    ) -> (Vec<PipelineEvent>, ()) {
        let events = complete_step(ctx, msg);
        self.schedule_drive(ctx); // a committed step result unblocks the next drive
        (events, ())
    }
}

/// A step's effect failed: release its launch claim so the next drive re-launches
/// it (the alarm-free core of a `retry`; a real backoff arms an alarm first).
#[derive(Clone, Serialize, Deserialize)]
struct Retry {
    id: u32,
}
impl Message for Retry {
    type Reply = ();
    const MANIFEST: Manifest = Manifest::new("test.Retry");
}
impl GrainHandler<Retry> for Pipeline {
    async fn handle(
        &self,
        _s: &PipelineState,
        msg: Retry,
        ctx: &GrainCtx<Self>,
    ) -> (Vec<PipelineEvent>, ()) {
        self.eph.lock().unwrap().guard.release(msg.id);
        self.schedule_drive(ctx);
        (vec![], ())
    }
}

#[derive(Clone, Serialize, Deserialize)]
struct Read;
impl Message for Read {
    type Reply = Option<u32>;
    const MANIFEST: Manifest = Manifest::new("test.Read");
}
impl GrainHandler<Read> for Pipeline {
    async fn handle(
        &self,
        state: &PipelineState,
        _m: Read,
        _ctx: &GrainCtx<Self>,
    ) -> (Vec<PipelineEvent>, Option<u32>) {
        (vec![], state.finished)
    }
}

// --- Test rig -----------------------------------------------------------------

fn rig(
    seed: u64,
    idle_after: Duration,
    fx: Effects,
) -> (Simulation, Recorder, granary::Granary<Pipeline>) {
    let sim = Simulation::new(seed);
    let recorder = Recorder::new();
    let sink: Arc<dyn EventSink> = Arc::new(recorder.clone());
    let system = LocalSystemBuilder::new(sim.clock(), sim.entropy(), sim.spawner())
        .events(sink)
        .build();
    let factory_fx = fx.clone();
    let grains = system.granary_named::<Pipeline>(
        Pipeline::GRAIN_TYPE,
        GranaryConfig {
            idle_after,
            ..GranaryConfig::default()
        },
        Arc::new(move || Pipeline {
            fx: factory_fx.clone(),
            eph: Mutex::new(Ephemeral::default()),
        }),
    );
    (sim, recorder, grains)
}

fn passivated(recorder: &Recorder) -> bool {
    recorder.events().iter().any(|e| {
        matches!(
            e.as_app::<GrainEvent>(),
            Some(GrainEvent::Passivated { .. })
        )
    })
}

// --- Tests --------------------------------------------------------------------

#[test]
fn workflow_runs_steps_sleeps_and_finishes() {
    let fx = Effects::default();
    let (sim, _rec, grains) = rig(1, Duration::from_secs(3600), fx.clone());
    let g = grains.grain("p/0");
    sim.block_on(async move {
        g.ask(Start).await.expect("start");
    });
    // block_on(Start) returns at t=0; the workflow drives on its own timers.
    sim.run();

    let g = grains.grain("p/0");
    let result = sim.block_on(async move { g.ask(Read).await.expect("read") });
    assert_eq!(result, Some(42), "fetch(21) → sleep → double = 42");
    assert_eq!(fx.fetch_runs.load(Ordering::SeqCst), 1, "fetch ran once");
    assert_eq!(fx.double_runs.load(Ordering::SeqCst), 1, "double ran once");
}

#[test]
fn steps_memoize_across_passivation() {
    // Aggressive idle window: the grain hibernates in every gap the workflow allows
    // (after Finished, and — because the alarm veto holds during the sleep — the
    // memo must still carry completed steps across the reactivations the drive
    // triggers). The witness is the run counters: each effect fires exactly once
    // even though the grain activates several times.
    let fx = Effects::default();
    let (sim, recorder, grains) = rig(2, Duration::from_millis(5), fx.clone());
    let g = grains.grain("p/0");
    sim.block_on(async move {
        g.ask(Start).await.expect("start");
    });
    sim.run();

    let g = grains.grain("p/0");
    let result = sim.block_on(async move { g.ask(Read).await.expect("read") });
    assert_eq!(result, Some(42));
    assert!(
        passivated(&recorder),
        "the aggressive idle window must have hibernated the grain at least once",
    );
    assert_eq!(
        fx.fetch_runs.load(Ordering::SeqCst),
        1,
        "memoization: the fetch effect runs once despite re-activations",
    );
    assert_eq!(fx.double_runs.load(Ordering::SeqCst), 1, "double runs once");
}

#[test]
fn retry_relaunches_after_a_failed_step() {
    // The first fetch launch fails (records no result); the re-drive re-launches it
    // and the second attempt records 21. The effect-run counter counts only
    // successful runs, so it lands at 1, and the workflow still finishes at 42.
    let fx = Effects::default();
    fx.fail_first_fetch.store(1, Ordering::SeqCst);
    let (sim, _rec, grains) = rig(3, Duration::from_secs(3600), fx.clone());
    let g = grains.grain("p/0");
    sim.block_on(async move {
        g.ask(Start).await.expect("start");
    });
    sim.run();

    let g = grains.grain("p/0");
    let result = sim.block_on(async move { g.ask(Read).await.expect("read") });
    assert_eq!(
        result,
        Some(42),
        "a failed step re-launches and the workflow completes"
    );
    assert_eq!(
        fx.fetch_runs.load(Ordering::SeqCst),
        1,
        "one successful fetch after the failed attempt"
    );
}
