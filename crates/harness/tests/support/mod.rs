//! Test support for the harness conformance suite (harness spec §11, §12).
//!
//! The harness adds **two** rows to the table granary already virtualizes
//! (§12.1): the **scripted model** (a deterministic function of the request) and
//! the **scripted sandbox** (deterministic outcomes per call). The journal is
//! the grain's, so the harness tests drive the real granary host, gateway, and
//! rehydration code under the same seed — simulating the harness *runs the real
//! consensus path*, with the agent's loop the only new code on it.
//!
//! Plus the continuous H-invariant checkers (§11) and the machine-readable
//! [`harness_catalogue`], guarded by the same drift-test pattern as the core and
//! granary catalogues. Activation, deactivation, and the single-writer fence are
//! observed through the **grain's** events (`Activated`/`Passivated`, granary
//! §13), not duplicated by the harness.

#![allow(dead_code)]

use std::collections::BTreeMap;
use std::collections::BTreeSet;
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::atomic::AtomicBool;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;
use std::time::Duration;

use actor_core::BoxFuture;
use actor_core::Clock;
use actor_core::Entropy;
use actor_core::Event;
use actor_core::LocalSystemBuilder;
use actor_core::NodeId;
use actor_simulation::CatalogueEntry;
use actor_simulation::Invariant;
use actor_simulation::SimClock;
use actor_simulation::SimEntropy;
use actor_simulation::SimSystem;
use actor_simulation::Simulation;
use actor_simulation::Verify;
use actor_simulation::default_invariants;
use granary::GrainEvent;
use granary::GrainName;
use harness::Harness;
use harness::HarnessConfig;
use harness::HarnessEvent;
use harness::Kinds;
use harness::Model;
use harness::ModelError;
use harness::ModelRequest;
use harness::ModelResponse;
use harness::Sandbox;
use harness::SandboxError;
use harness::SandboxProfile;
use harness::SandboxProvider;
use harness::SessionId;
use harness::Tier;
use harness::ToolCall;
use harness::ToolError;
use harness::TurnId;
use harness::Usage;
use serde_json::Value;

/// Coerce an async block to the `BoxFuture` the workload traits want.
pub fn boxed(f: impl std::future::Future<Output = ()> + Send + 'static) -> BoxFuture<'static, ()> {
    Box::pin(f)
}

/// Build a single-node simulation, a `LocalSystem` on it, and a harness hosting
/// `kinds` with the given seams — the common setup for the scenario tests.
pub fn harness_on(
    sim: &Simulation,
    kinds: Kinds,
    model: Arc<dyn Model>,
    sandboxes: Arc<dyn SandboxProvider>,
) -> Harness<SimSystem> {
    let system = LocalSystemBuilder::new(sim.clock(), sim.entropy(), sim.spawner()).build();
    Harness::cluster(system, &kinds, model, sandboxes)
}

/// Read a session's whole journal via `Tail` (§10.2) — the journal is the
/// grain's, so the transcript is read back through the activation, not a
/// test-held store.
pub async fn tail_records(session: &harness::SessionRef<SimSystem>) -> Vec<harness::Record> {
    session
        .tail(granary::Seq::new(0), 1_000_000)
        .await
        .expect("tail")
        .into_iter()
        .map(|(_, record)| record)
        .collect()
}

/// A short label for a record body, for asserting write-ahead order (§6.4).
pub fn record_kind(body: &harness::RecordBody) -> &'static str {
    match body {
        harness::RecordBody::SessionCreated { .. } => "created",
        harness::RecordBody::TurnSubmitted { .. } => "turn",
        harness::RecordBody::ModelResponse { .. } => "model",
        harness::RecordBody::ToolOutcome { .. } => "tool",
        harness::RecordBody::ChildRun { .. } => "child",
        harness::RecordBody::WorkspaceReset => "reset",
        harness::RecordBody::TierAcquired { .. } => "tier",
        harness::RecordBody::RunEnded { .. } => "ended",
    }
}

/// The record-body labels of a session's journal, in order.
pub fn record_kinds(records: &[harness::Record]) -> Vec<&'static str> {
    records.iter().map(|r| record_kind(&r.body)).collect()
}

type ScenarioBody =
    Arc<dyn Fn(Harness<SimSystem>, SimSystem) -> BoxFuture<'static, ()> + Send + Sync>;
type ModelFactory = Arc<dyn Fn(&SimSystem) -> Arc<dyn Model> + Send + Sync>;
type SandboxFactory = Arc<dyn Fn(&SimSystem) -> Arc<dyn SandboxProvider> + Send + Sync>;

/// A reusable single-run workload (§12): builds a harness on the simulation's
/// `LocalSystem` with the given seams, runs `body`, and checks the harness
/// invariants over the run's event stream. The journal is the grain's; the seams
/// come from factories so a seam that needs the run's clock (a `SlowModel`) is
/// built from the handed system, while a caller-held seam (whose stats are read
/// after the run) is captured and cloned by a constant factory ([`Scenario::new`]).
pub struct Scenario {
    name: &'static str,
    kinds: Kinds,
    model: ModelFactory,
    sandboxes: SandboxFactory,
    config: HarnessConfig,
    body: ScenarioBody,
}

impl Scenario {
    pub fn new(
        name: &'static str,
        kinds: Kinds,
        model: Arc<dyn Model>,
        sandboxes: Arc<dyn SandboxProvider>,
        body: impl Fn(Harness<SimSystem>, SimSystem) -> BoxFuture<'static, ()> + Send + Sync + 'static,
    ) -> Scenario {
        Scenario::from_factories(
            name,
            kinds,
            Arc::new(move |_| Arc::clone(&model)),
            Arc::new(move |_| Arc::clone(&sandboxes)),
            body,
        )
    }

    pub fn from_factories(
        name: &'static str,
        kinds: Kinds,
        model: ModelFactory,
        sandboxes: SandboxFactory,
        body: impl Fn(Harness<SimSystem>, SimSystem) -> BoxFuture<'static, ()> + Send + Sync + 'static,
    ) -> Scenario {
        Scenario {
            name,
            kinds,
            model,
            sandboxes,
            config: HarnessConfig::default(),
            body: Arc::new(body),
        }
    }

    pub fn with_config(mut self, config: HarnessConfig) -> Scenario {
        self.config = config;
        self
    }
}

impl actor_simulation::Workload for Scenario {
    fn name(&self) -> &'static str {
        self.name
    }

    fn run(&self, system: SimSystem) -> BoxFuture<'static, ()> {
        let model = (self.model)(&system);
        let sandboxes = (self.sandboxes)(&system);
        let harness = Harness::builder(system.clone(), &self.kinds)
            .config(self.config.clone())
            .host_all(model, sandboxes)
            .build();
        (self.body)(harness, system)
    }

    fn invariants(&self) -> Vec<Box<dyn Invariant>> {
        harness_invariants()
    }
}

/// A kind config with an aggressive idle window, so a single test run exercises
/// idle hibernation and sandbox release (§7.2, H8).
pub fn brisk_idle() -> granary::GranaryConfig {
    granary::GranaryConfig {
        idle_after: Duration::from_secs(1),
        // The agent's workspace facet materializes a real directory per grain
        // (granary §7.11); a fresh tempdir per kind map keeps parallel tests
        // from sharing scratch paths. Kept (not dropped) so it outlives the
        // config; the OS reaps the temp tree.
        data_dir: Some(
            tempfile::tempdir()
                .expect("workspace scratch tempdir")
                .keep(),
        ),
        ..granary::GranaryConfig::default()
    }
}

// ---------------------------------------------------------------------------
// Scripted model (§12.1)
// ---------------------------------------------------------------------------

type ModelScript = Arc<dyn Fn(&ModelRequest) -> Result<ModelResponse, ModelError> + Send + Sync>;
type SandboxScript = Arc<dyn Fn(&str, &Value) -> Result<Value, ToolError> + Send + Sync>;

/// A deterministic model: a pure function of the request (§4.2 rule 2).
#[derive(Clone)]
pub struct ScriptedModel {
    script: ModelScript,
}

impl ScriptedModel {
    pub fn new(
        script: impl Fn(&ModelRequest) -> Result<ModelResponse, ModelError> + Send + Sync + 'static,
    ) -> ScriptedModel {
        ScriptedModel {
            script: Arc::new(script),
        }
    }

    /// A script keyed by step: the n-th model call of a run gets `responses[n]`;
    /// past the end, a final "done" message. The step index is the number of
    /// assistant entries in the transcript — a pure function of the request.
    pub fn steps(responses: Vec<Result<ModelResponse, ModelError>>) -> ScriptedModel {
        ScriptedModel::new(move |req| {
            let step = req
                .transcript
                .iter()
                .filter(|e| matches!(e, harness::Entry::Assistant { .. }))
                .count();
            responses
                .get(step)
                .cloned()
                .unwrap_or_else(|| Ok(final_message("done")))
        })
    }
}

impl Model for ScriptedModel {
    fn complete(&self, req: ModelRequest) -> BoxFuture<'static, Result<ModelResponse, ModelError>> {
        let result = (self.script)(&req);
        Box::pin(async move { result })
    }
}

/// A final assistant message with nominal usage.
pub fn final_message(text: &str) -> ModelResponse {
    ModelResponse {
        content: text.to_string(),
        calls: Vec::new(),
        usage: Usage {
            input_tokens: 100,
            output_tokens: 20,
        },
    }
}

/// A response requesting one tool call.
pub fn tool_call(id: &str, name: &str, input: Value) -> ModelResponse {
    ModelResponse {
        content: format!("calling {name}"),
        calls: vec![ToolCall {
            id: harness::CallId::new(id),
            name: name.to_string(),
            input,
        }],
        usage: Usage {
            input_tokens: 100,
            output_tokens: 30,
        },
    }
}

/// A model that takes a fixed span of logical time per call — for cancel races
/// (§9.2): the mailbox stays live during the call (§3.2), so a cancel lands at
/// message granularity.
pub struct SlowModel {
    pub inner: Arc<dyn Model>,
    pub clock: SimClock,
    pub delay: Duration,
}

impl Model for SlowModel {
    fn complete(&self, req: ModelRequest) -> BoxFuture<'static, Result<ModelResponse, ModelError>> {
        let clock = self.clock.clone();
        let delay = self.delay;
        let inner = Arc::clone(&self.inner);
        Box::pin(async move {
            clock.sleep(delay).await;
            inner.complete(req).await
        })
    }
}

/// An event sink that records everything, for tests that assert on specific
/// events rather than only invariants.
#[derive(Clone, Default)]
pub struct CollectingSink {
    events: Arc<Mutex<Vec<Event>>>,
}

impl CollectingSink {
    pub fn events(&self) -> Vec<Event> {
        self.events.lock().expect("events mutex").clone()
    }
}

impl actor_core::EventSink for CollectingSink {
    fn emit(&self, event: Event) {
        self.events
            .lock()
            .expect("events mutex")
            .push(event.clone());
    }
}

/// Feed a recorded stream through the harness checkers after the fact.
pub fn check_events(events: &[Event]) -> Vec<String> {
    let mut invariants = harness_invariants();
    let mut violations = Vec::new();
    for event in events {
        for inv in invariants.iter_mut() {
            if let Err(detail) = inv.observe(event) {
                violations.push(format!("[{}] {detail}", inv.name()));
            }
        }
    }
    for inv in invariants.iter_mut() {
        if let Err(detail) = inv.at_quiescence() {
            violations.push(format!("[{}] {detail}", inv.name()));
        }
    }
    violations
}

/// Wraps a model with seeded latency and failure bursts (§12.2): faults live
/// behind the same seam production uses, gated by the run's entropy.
pub struct FaultyModel {
    pub inner: Arc<dyn Model>,
    pub clock: SimClock,
    pub entropy: SimEntropy,
    pub max_latency: Duration,
    pub fail_num: u64,
    pub fail_den: u64,
    pub fired: Arc<AtomicUsize>,
}

impl Model for FaultyModel {
    fn complete(&self, req: ModelRequest) -> BoxFuture<'static, Result<ModelResponse, ModelError>> {
        let clock = self.clock.clone();
        let latency = if self.max_latency.is_zero() {
            Duration::ZERO
        } else {
            Duration::from_nanos(self.entropy.next_u64() % self.max_latency.as_nanos() as u64)
        };
        let fail = self.fail_num > 0 && self.entropy.buggify(self.fail_num, self.fail_den);
        let overloaded = self.entropy.next_u64().is_multiple_of(2);
        if fail {
            self.fired.fetch_add(1, Ordering::SeqCst);
        }
        let inner = Arc::clone(&self.inner);
        Box::pin(async move {
            clock.sleep(latency).await;
            if fail {
                return Err(if overloaded {
                    ModelError::Overloaded
                } else {
                    ModelError::RateLimited
                });
            }
            inner.complete(req).await
        })
    }
}

// ---------------------------------------------------------------------------
// Scripted sandbox (§12.1)
// ---------------------------------------------------------------------------

/// Observable provider activity, for H8 assertions.
#[derive(Clone, Default)]
pub struct SandboxStats {
    opened: Arc<AtomicUsize>,
    released: Arc<AtomicUsize>,
    calls: Arc<Mutex<Vec<(Tier, String, Value)>>>,
}

impl SandboxStats {
    pub fn opened(&self) -> usize {
        self.opened.load(Ordering::SeqCst)
    }

    pub fn released(&self) -> usize {
        self.released.load(Ordering::SeqCst)
    }

    pub fn calls(&self) -> Vec<(Tier, String, Value)> {
        self.calls.lock().expect("calls mutex").clone()
    }
}

/// A deterministic sandbox provider: outcomes per call from a closure, with
/// switchable open failure (§12.2).
#[derive(Clone)]
pub struct ScriptedSandboxes {
    behavior: SandboxScript,
    fail_open: Arc<AtomicBool>,
    delay: Option<(SimClock, Duration)>,
    durable: bool,
    pub stats: SandboxStats,
}

impl ScriptedSandboxes {
    pub fn new(
        behavior: impl Fn(&str, &Value) -> Result<Value, ToolError> + Send + Sync + 'static,
    ) -> ScriptedSandboxes {
        ScriptedSandboxes {
            behavior: Arc::new(behavior),
            fail_open: Arc::new(AtomicBool::new(false)),
            delay: None,
            durable: false,
            stats: SandboxStats::default(),
        }
    }

    pub fn with_delay(mut self, clock: SimClock, delay: Duration) -> ScriptedSandboxes {
        self.delay = Some((clock, delay));
        self
    }

    /// Echo every call back as `"{name}: ok"` — the do-nothing workspace.
    pub fn echo() -> ScriptedSandboxes {
        ScriptedSandboxes::new(|name, _| Ok(Value::String(format!("{name}: ok"))))
    }

    pub fn set_fail_open(&self, fail: bool) {
        self.fail_open.store(fail, Ordering::SeqCst);
    }
}

struct ScriptedSandbox {
    behavior: SandboxScript,
    delay: Option<(SimClock, Duration)>,
    stats: SandboxStats,
}

impl Sandbox for ScriptedSandbox {
    fn call(
        &self,
        tier: Tier,
        name: &str,
        input: Value,
    ) -> BoxFuture<'static, Result<Value, ToolError>> {
        self.stats
            .calls
            .lock()
            .expect("calls mutex")
            .push((tier, name.to_string(), input.clone()));
        let result = (self.behavior)(name, &input);
        let delay = self.delay.clone();
        Box::pin(async move {
            if let Some((clock, delay)) = delay {
                clock.sleep(delay).await;
            }
            result
        })
    }

    fn release(&self) -> BoxFuture<'static, ()> {
        self.stats.released.fetch_add(1, Ordering::SeqCst);
        Box::pin(async move {})
    }
}

impl SandboxProvider for ScriptedSandboxes {
    fn open(
        &self,
        _session: &SessionId,
        _profile: &SandboxProfile,
        _workspace: &std::path::Path,
    ) -> BoxFuture<'static, Result<Arc<dyn Sandbox>, SandboxError>> {
        let result: Result<Arc<dyn Sandbox>, SandboxError> =
            if self.fail_open.load(Ordering::SeqCst) {
                Err(SandboxError("scripted open failure".to_string()))
            } else {
                self.stats.opened.fetch_add(1, Ordering::SeqCst);
                Ok(Arc::new(ScriptedSandbox {
                    behavior: Arc::clone(&self.behavior),
                    delay: self.delay.clone(),
                    stats: self.stats.clone(),
                }))
            };
        Box::pin(async move { result })
    }
}

// ---------------------------------------------------------------------------
// Continuous H-invariant checkers (§11)
// ---------------------------------------------------------------------------

/// The session a grain event names, by its key (§2.2: the `GrainName` key is the
/// `SessionId`). Tests use unique session keys, so the key identifies the run.
fn session_of(name: &GrainName) -> SessionId {
    SessionId::new(name.key())
}

/// **Effect containment and single per-node activation** (H6 per-node half, H8):
/// activation is the grain's (`Activated`/`Passivated` strictly alternate per
/// session and node, granary §13); the harness's `SandboxBound`/`SandboxReleased`
/// alternate **within** that window and the sandbox is released before
/// deactivation; and a `ModelCompleted` only fires inside a live activation.
#[derive(Default)]
pub struct HarnessEventGrammar {
    /// (session, node) → (active, sandbox_bound)
    windows: BTreeMap<(SessionId, NodeId), (bool, bool)>,
}

impl HarnessEventGrammar {
    fn window(&mut self, session: SessionId, node: NodeId) -> &mut (bool, bool) {
        self.windows
            .entry((session, node))
            .or_insert((false, false))
    }
}

impl Invariant for HarnessEventGrammar {
    fn name(&self) -> &'static str {
        "harness-event-grammar"
    }

    fn observe(&mut self, event: &Event) -> Result<(), String> {
        // Activation lifecycle from the grain's own events (granary §13).
        if let Some(grain) = event.as_app::<GrainEvent>() {
            match grain {
                GrainEvent::Activated { node, name } => {
                    let w = self.window(session_of(name), *node);
                    if w.0 {
                        return Err(format!(
                            "second activation of {name} on {node} without passivation (G6)"
                        ));
                    }
                    *w = (true, false);
                }
                GrainEvent::Passivated { node, name } => {
                    let w = self.window(session_of(name), *node);
                    if w.1 {
                        return Err(format!(
                            "passivation of {name} on {node} with the sandbox still bound (H8)"
                        ));
                    }
                    *w = (false, false);
                }
                _ => {}
            }
            return Ok(());
        }
        let Some(event) = event.as_app::<HarnessEvent>() else {
            return Ok(());
        };
        match event {
            HarnessEvent::SandboxBound { session, node } => {
                let w = self.window(session.clone(), *node);
                if !w.0 {
                    return Err(format!(
                        "sandbox bound for {session} on {node} outside an activation (H8)"
                    ));
                }
                if w.1 {
                    return Err(format!("second sandbox bound for {session} on {node} (H8)"));
                }
                w.1 = true;
            }
            HarnessEvent::SandboxReleased { session, node } => {
                let w = self.window(session.clone(), *node);
                if !w.1 {
                    return Err(format!(
                        "sandbox released for {session} on {node} without a bind (H8)"
                    ));
                }
                w.1 = false;
            }
            HarnessEvent::ModelCompleted { session, node, .. } => {
                let w = self.window(session.clone(), *node);
                if !w.0 {
                    return Err(format!(
                        "model completion for {session} on {node} outside an activation (H6)"
                    ));
                }
            }
            _ => {}
        }
        Ok(())
    }
}

/// **Run discipline** (H3 pairing, H7): per `(session, turn)` exactly one
/// `RunStarted` and at most one `RunEnded`, never an end without a start. A
/// resume emits no second `RunStarted` (§10.4), and `ModelCompleted` is scoped to
/// journaled spend (emitted only after the response commits, §9.1.4), so no
/// completion follows a run's end.
#[derive(Default)]
pub struct RunDiscipline {
    started: BTreeSet<(SessionId, TurnId)>,
    ended: BTreeSet<(SessionId, TurnId)>,
}

impl Invariant for RunDiscipline {
    fn name(&self) -> &'static str {
        "harness-run-discipline"
    }

    fn observe(&mut self, event: &Event) -> Result<(), String> {
        let Some(event) = event.as_app::<HarnessEvent>() else {
            return Ok(());
        };
        match event {
            HarnessEvent::RunStarted { session, turn, .. }
                if !self.started.insert((session.clone(), turn.clone())) =>
            {
                return Err(format!("second RunStarted for {session}/{turn} (H7)"));
            }
            HarnessEvent::RunEnded { session, turn, .. } => {
                let key = (session.clone(), turn.clone());
                if !self.started.contains(&key) {
                    return Err(format!(
                        "RunEnded without RunStarted for {session}/{turn} (H3)"
                    ));
                }
                if !self.ended.insert(key) {
                    return Err(format!("second RunEnded for {session}/{turn} (H3)"));
                }
            }
            HarnessEvent::ModelCompleted { session, turn, .. }
                if self.ended.contains(&(session.clone(), turn.clone())) =>
            {
                return Err(format!(
                    "model call for {session}/{turn} completed after the run ended (H4/H5)"
                ));
            }
            HarnessEvent::ToolCompleted { session, turn, .. }
                if self.ended.contains(&(session.clone(), turn.clone())) =>
            {
                return Err(format!(
                    "tool call for {session}/{turn} completed after the run ended (§3.2)"
                ));
            }
            _ => {}
        }
        Ok(())
    }
}

/// The default checker set for harness workloads: the core and grain invariants
/// plus the harness's continuous H-checkers (§11).
pub fn harness_invariants() -> Vec<Box<dyn Invariant>> {
    let mut invariants = default_invariants();
    invariants.push(Box::new(HarnessEventGrammar::default()));
    invariants.push(Box::new(RunDiscipline::default()));
    invariants
}

// ---------------------------------------------------------------------------
// S4 journal audit (sandbox spec §6)
// ---------------------------------------------------------------------------

/// The S4 journal audit (sandbox spec §6 S4): within one session's journal,
/// every executed tool outcome at a tier other than `Workspace` is preceded by a
/// `TierAcquired` at that tier with no `WorkspaceReset` in between, and every
/// acquired or executed tier lies within the kind's cap.
///
/// Synthesized outcomes — an unknown name, schema-rejected arguments (§5.4), or a
/// dangling call resolved as `Interrupted` on resume (§5.5) — carry no effect and
/// are exempt.
pub fn audit_tier_acquisition(records: &[harness::Record], kind: &harness::Kind) {
    let cap = kind.tier_cap();
    let mut call_tier: BTreeMap<harness::CallId, Tier> = BTreeMap::new();
    let mut held: BTreeSet<Tier> = BTreeSet::from([Tier::Workspace]);
    for (position, record) in records.iter().enumerate() {
        match &record.body {
            harness::RecordBody::ModelResponse { calls, .. } => {
                for call in calls {
                    if let Some(decl) = kind.tools.get(&call.name) {
                        call_tier.insert(call.id.clone(), decl.tier);
                    }
                }
            }
            harness::RecordBody::TierAcquired { tier, .. } => {
                assert!(
                    cap.contains(tier),
                    "S4: record {position} acquires tier {tier:?} outside the cap {cap:?}"
                );
                held.insert(*tier);
            }
            harness::RecordBody::WorkspaceReset => {
                held = BTreeSet::from([Tier::Workspace]);
            }
            harness::RecordBody::ToolOutcome { call, outcome, .. } => {
                let executed = !matches!(
                    outcome,
                    Err(ToolError::UnknownTool { .. })
                        | Err(ToolError::InvalidArguments(_))
                        | Err(ToolError::Interrupted)
                );
                let Some(tier) = call_tier.get(call) else {
                    continue;
                };
                if executed {
                    assert!(
                        cap.contains(tier),
                        "S4: record {position} executes call {call:?} at tier {tier:?} outside the cap {cap:?}"
                    );
                    assert!(
                        held.contains(tier),
                        "S4: record {position} executes call {call:?} at tier {tier:?} with no preceding TierAcquired in this reset segment"
                    );
                }
            }
            _ => {}
        }
    }
}

// ---------------------------------------------------------------------------
// The H catalogue (§11)
// ---------------------------------------------------------------------------

/// The harness invariant catalogue (harness spec §11): machine readable
/// alongside the conformance suite, guarded by the drift test in
/// `conformance_catalogue.rs`. `invariant: n` reads as "Hn". **H2 is retired**
/// (§11): the single-writer fence is wholly the grain's (G1), so it has no
/// harness invariant; the numbers H3–H8 are kept stable rather than renumbered.
pub fn harness_catalogue() -> &'static [CatalogueEntry] {
    HARNESS_CATALOGUE
}

const HARNESS_CATALOGUE: &[CatalogueEntry] = &[
    CatalogueEntry {
        invariant: 1,
        spec: "harness §6.2, §7.5",
        property: "Deterministic fold and resume: state is a pure fold of the journal; a session resumed from any committed prefix behaves identically to one that never stopped, given the same subsequent outcomes",
        verify: &[Verify::SimTest(
            "harness/tests/conformance_resume.rs (differential), harness/tests/reproducibility.rs (seed sweep)",
        )],
    },
    CatalogueEntry {
        invariant: 3,
        spec: "harness §3.1, §7.5, §9",
        property: "Run termination: every RunStarted is followed by exactly one RunEnded; once faults cease and a caller re-contacts the session, no run remains pending past its budget's bound",
        verify: &[
            Verify::Checker("harness-run-discipline"),
            Verify::SimTest("harness/tests/conformance_run.rs, harness/tests/cluster.rs"),
        ],
    },
    CatalogueEntry {
        invariant: 4,
        spec: "harness §9.1",
        property: "Budget bound: no model call after exhaustion; output spend never exceeds the remainder at call time; own spend plus carve-outs never exceeds the budget at every tree level",
        verify: &[
            Verify::Checker("harness-run-discipline"),
            Verify::SimTest("harness/tests/conformance_budget.rs (journal audit, tree scenario)"),
        ],
    },
    CatalogueEntry {
        invariant: 5,
        spec: "harness §9.2",
        property: "Cancellation: after a cancel is journaled, the run and (once faults cease) every descendant run end Cancelled in bounded logical time, issuing no further model calls",
        verify: &[
            Verify::Checker("harness-run-discipline"),
            Verify::SimTest("harness/tests/conformance_cancel.rs"),
        ],
    },
    CatalogueEntry {
        invariant: 6,
        spec: "harness §7.2, §7.5",
        property: "Single autonomous activation: per-node activations never overlap; a converged healed cluster runs at most one activation per session; an addressed session activates within bounded logical time of contact",
        verify: &[
            Verify::Checker("harness-event-grammar"),
            Verify::SimTest("harness/tests/cluster.rs (converged and liveness halves)"),
        ],
    },
    CatalogueEntry {
        invariant: 7,
        spec: "harness §7.4",
        property: "Idempotent submission: a re-submitted TurnId never starts a second run — it returns the recorded outcome or attaches to the live run",
        verify: &[
            Verify::Checker("harness-run-discipline"),
            Verify::SimTest("harness/tests/conformance_run.rs (retry scenarios)"),
        ],
    },
    CatalogueEntry {
        invariant: 8,
        spec: "harness §5.3, §5.5",
        property: "Effect containment: at most one live sandbox per activation, released on deactivation; sandboxed calls execute in their activation's sandbox; loss surfaces in the journal, never as silent corruption",
        verify: &[
            Verify::Checker("harness-event-grammar"),
            Verify::SimTest("harness/tests/conformance_sandbox.rs (crash/loss scenarios)"),
        ],
    },
];
