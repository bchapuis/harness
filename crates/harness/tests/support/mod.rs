//! Test support for the harness conformance suite (harness spec §11, §12).
//!
//! The harness's three rows of the virtualization table (§12.1): the
//! **scripted model** (a deterministic function of the request), the
//! **scripted sandbox** (deterministic outcomes per call), and the
//! **faulted journal** (the in-memory journal wrapped with seeded latency,
//! `Unavailable` windows, and per-op failures). Plus the continuous
//! H-invariant checkers (§11) and the machine-readable
//! [`harness_catalogue`], guarded by the same drift-test pattern as the core
//! and utilities catalogues.

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
use actor_core::NodeId;
use actor_simulation::CatalogueEntry;
use actor_simulation::Invariant;
use actor_simulation::SimClock;
use actor_simulation::SimEntropy;
use actor_simulation::Verify;
use actor_simulation::default_invariants;
use harness::AppendError;
use harness::HarnessEvent;
use harness::Journal;
use harness::JournalError;
use harness::Model;
use harness::ModelError;
use harness::ModelRequest;
use harness::ModelResponse;
use harness::Record;
use harness::Sandbox;
use harness::SandboxError;
use harness::SandboxProfile;
use harness::SandboxProvider;
use harness::SeqNo;
use harness::SessionId;
use harness::ToolCall;
use harness::ToolError;
use harness::TurnId;
use harness::Usage;
use serde_json::Value;

/// Coerce an async block to the `BoxFuture` the workload traits want — a
/// struct-literal initializer provides no expected type, so the coercion
/// needs a hand.
pub fn boxed(f: impl std::future::Future<Output = ()> + Send + 'static) -> BoxFuture<'static, ()> {
    Box::pin(f)
}

// ---------------------------------------------------------------------------
// Scripted model (§12.1)
// ---------------------------------------------------------------------------

/// The scripted model's behavior: a pure function of the request.
type ModelScript = Arc<dyn Fn(&ModelRequest) -> Result<ModelResponse, ModelError> + Send + Sync>;
/// The scripted sandbox's behavior: a pure function of the call.
type SandboxScript = Arc<dyn Fn(&str, &Value) -> Result<Value, ToolError> + Send + Sync>;

/// A deterministic model: a pure function of the request (§4.2 rule 2).
/// Everything downstream of a fixed response sequence is reproducible (H1).
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

    /// A script keyed by step: the n-th model call of a run gets
    /// `responses[n]`; past the end, a final "done" message. The step index
    /// is the number of assistant entries in the transcript — a pure
    /// function of the request, as §12.1 demands.
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

/// A model that takes a fixed span of logical time per call — for cancel
/// races (§9.2): the mailbox stays live during the call (§3.2), so a cancel
/// lands at message granularity.
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
/// harness events (the fence race, resume) rather than only invariants.
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

/// Feed a recorded stream through the harness checkers after the fact — for
/// manually assembled runs that bypass `run_seed`.
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
    /// Per-call failure probability `num / den`; zero disables.
    pub fail_num: u64,
    pub fail_den: u64,
    /// Failures actually injected — coverage accounting (§11): a sweep that
    /// configures faults but never fires one gives false confidence.
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
        let overloaded = self.entropy.next_u64() % 2 == 0;
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

/// Observable provider activity, for H8 assertions: every environment opened
/// is eventually released, exactly once.
#[derive(Clone, Default)]
pub struct SandboxStats {
    opened: Arc<AtomicUsize>,
    released: Arc<AtomicUsize>,
    calls: Arc<Mutex<Vec<(String, Value)>>>,
}

impl SandboxStats {
    pub fn opened(&self) -> usize {
        self.opened.load(Ordering::SeqCst)
    }

    pub fn released(&self) -> usize {
        self.released.load(Ordering::SeqCst)
    }

    pub fn calls(&self) -> Vec<(String, Value)> {
        self.calls.lock().expect("calls mutex").clone()
    }
}

/// A deterministic sandbox provider: outcomes per call from a closure, with
/// switchable open failure (§12.2).
#[derive(Clone)]
pub struct ScriptedSandboxes {
    behavior: SandboxScript,
    fail_open: Arc<AtomicBool>,
    /// Logical time each call takes (with the clock to take it from) — for
    /// per-tool timeout scenarios (§5.3 item 3).
    delay: Option<(SimClock, Duration)>,
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
            stats: SandboxStats::default(),
        }
    }

    pub fn with_delay(mut self, clock: SimClock, delay: Duration) -> ScriptedSandboxes {
        self.delay = Some((clock, delay));
        self
    }

    /// Echo every call back as `{"tool": name}` — the do-nothing workspace.
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
    fn call(&self, name: &str, input: Value) -> BoxFuture<'static, Result<Value, ToolError>> {
        self.stats
            .calls
            .lock()
            .expect("calls mutex")
            .push((name.to_string(), input.clone()));
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
// Faulted journal (§12.1, §12.2)
// ---------------------------------------------------------------------------

/// The in-memory journal wrapped with seeded latency, switchable
/// `Unavailable` windows, and per-op seeded failures — the substrate the
/// §12.2 journal faults inject through.
#[derive(Clone)]
pub struct FaultedJournal {
    pub inner: harness::InMemoryJournal,
    pub clock: SimClock,
    pub entropy: SimEntropy,
    pub max_latency: Duration,
    unavailable: Arc<AtomicBool>,
    /// Per-op `Unavailable` probability `num / den`; zero disables.
    pub fail_num: u64,
    pub fail_den: u64,
    /// Failures actually injected — coverage accounting (§11).
    pub fired: Arc<AtomicUsize>,
}

impl FaultedJournal {
    pub fn new(inner: harness::InMemoryJournal, clock: SimClock, entropy: SimEntropy) -> Self {
        FaultedJournal {
            inner,
            clock,
            entropy,
            max_latency: Duration::ZERO,
            unavailable: Arc::new(AtomicBool::new(false)),
            fail_num: 0,
            fail_den: 1,
            fired: Arc::new(AtomicUsize::new(0)),
        }
    }

    pub fn with_latency(mut self, max: Duration) -> Self {
        self.max_latency = max;
        self
    }

    pub fn with_failures(mut self, num: u64, den: u64) -> Self {
        self.fail_num = num;
        self.fail_den = den;
        self
    }

    /// Open or close an outage window (§12.2): every op fails `Unavailable`
    /// while open.
    pub fn set_unavailable(&self, unavailable: bool) {
        self.unavailable.store(unavailable, Ordering::SeqCst);
    }

    fn latency(&self) -> Duration {
        if self.max_latency.is_zero() {
            Duration::ZERO
        } else {
            Duration::from_nanos(self.entropy.next_u64() % self.max_latency.as_nanos() as u64)
        }
    }

    fn should_fail(&self) -> bool {
        let fail = self.unavailable.load(Ordering::SeqCst)
            || (self.fail_num > 0 && self.entropy.buggify(self.fail_num, self.fail_den));
        if fail {
            self.fired.fetch_add(1, Ordering::SeqCst);
        }
        fail
    }
}

impl Journal for FaultedJournal {
    fn append(
        &self,
        session: &SessionId,
        after: SeqNo,
        records: Vec<Record>,
    ) -> BoxFuture<'static, Result<SeqNo, AppendError>> {
        let clock = self.clock.clone();
        let latency = self.latency();
        let fail = self.should_fail();
        let inner = self.inner.clone();
        let session = session.clone();
        Box::pin(async move {
            clock.sleep(latency).await;
            if fail {
                return Err(AppendError::Unavailable("injected outage".to_string()));
            }
            inner.append(&session, after, records).await
        })
    }

    fn load(
        &self,
        session: &SessionId,
        from: SeqNo,
        limit: usize,
    ) -> BoxFuture<'static, Result<Vec<(SeqNo, Record)>, JournalError>> {
        let clock = self.clock.clone();
        let latency = self.latency();
        let fail = self.should_fail();
        let inner = self.inner.clone();
        let session = session.clone();
        Box::pin(async move {
            clock.sleep(latency).await;
            if fail {
                return Err(JournalError::Unavailable("injected outage".to_string()));
            }
            inner.load(&session, from, limit).await
        })
    }
}

// ---------------------------------------------------------------------------
// Continuous H-invariant checkers (§11)
// ---------------------------------------------------------------------------

/// **Harness event grammar** (H2, H6 per-node half, H8): per session and
/// node, `SessionActivated`/`SessionDeactivated` strictly alternate;
/// `SandboxBound`/`SandboxReleased` alternate within the activation;
/// harness activity only happens inside an activation; and an
/// `AppendRejected` is followed by that node's `SessionDeactivated` with no
/// intervening harness activity for the session (§10.4).
///
/// Harness events ride the shared stream as `Event::App` (core spec §16);
/// the checker recovers them by downcast and ignores everything else.
#[derive(Default)]
pub struct HarnessEventGrammar {
    /// (session, node) → (active, sandbox_bound, fenced)
    windows: BTreeMap<(SessionId, NodeId), (bool, bool, bool)>,
}

impl HarnessEventGrammar {
    fn window(&mut self, session: &SessionId, node: NodeId) -> &mut (bool, bool, bool) {
        self.windows
            .entry((session.clone(), node))
            .or_insert((false, false, false))
    }
}

impl Invariant for HarnessEventGrammar {
    fn name(&self) -> &'static str {
        "harness-event-grammar"
    }

    fn observe(&mut self, event: &Event) -> Result<(), String> {
        let Some(event) = event.as_app::<HarnessEvent>() else {
            return Ok(());
        };
        match event {
            HarnessEvent::SessionActivated { session, node } => {
                let w = self.window(session, *node);
                if w.0 {
                    return Err(format!(
                        "second activation of {session} on {node} without deactivation (H6)"
                    ));
                }
                *w = (true, false, false);
            }
            HarnessEvent::SessionDeactivated { session, node } => {
                let w = self.window(session, *node);
                if !w.0 {
                    return Err(format!(
                        "deactivation of {session} on {node} without activation (H6)"
                    ));
                }
                if w.1 {
                    return Err(format!(
                        "deactivation of {session} on {node} with the sandbox still bound (H8)"
                    ));
                }
                *w = (false, false, false);
            }
            HarnessEvent::AppendRejected { session, node } => {
                let w = self.window(session, *node);
                if !w.0 {
                    return Err(format!(
                        "fence rejection for {session} on {node} outside an activation (H2)"
                    ));
                }
                w.2 = true;
            }
            HarnessEvent::SandboxBound { session, node } => {
                let w = self.window(session, *node);
                if !w.0 {
                    return Err(format!(
                        "sandbox bound for {session} on {node} outside an activation (H8)"
                    ));
                }
                if w.1 {
                    return Err(format!("second sandbox bound for {session} on {node} (H8)"));
                }
                if w.2 {
                    return Err(format!(
                        "sandbox bound for {session} on {node} after a fence rejection (H2)"
                    ));
                }
                w.1 = true;
            }
            HarnessEvent::SandboxReleased { session, node } => {
                let w = self.window(session, *node);
                if !w.1 {
                    return Err(format!(
                        "sandbox released for {session} on {node} without a bind (H8)"
                    ));
                }
                w.1 = false;
            }
            HarnessEvent::RunResumed { session, node, .. } => {
                let w = self.window(session, *node);
                if !w.0 || w.2 {
                    return Err(format!(
                        "run resumed for {session} on {node} outside a live activation (H2/H6)"
                    ));
                }
            }
            HarnessEvent::ModelCompleted { session, node, .. } => {
                let w = self.window(session, *node);
                if !w.0 {
                    return Err(format!(
                        "model completion for {session} on {node} outside an activation (H6)"
                    ));
                }
                if w.2 {
                    return Err(format!(
                        "model completion for {session} on {node} after a fence rejection (H2)"
                    ));
                }
            }
            _ => {}
        }
        Ok(())
    }

    // No at-quiescence sweep: a time-bounded cluster run (`run_for`) stops
    // mid-flight, where a live activation legitimately still holds its
    // sandbox — the same reason the core keeps mailbox-depth checks out of
    // the continuous set. H8's checkable content is the alternation and the
    // release-before-deactivation ordering, both enforced per event above;
    // actual release counts are asserted by the scenario tests.
}

/// **Run discipline** (H3 pairing, H4/H5 post-end silence, H7): per
/// `(session, turn)` exactly one `RunStarted` and at most one `RunEnded`,
/// never an end without a start — and no unfenced activation completes a
/// model call for a turn after its terminal outcome (§10.4 scopes the spend
/// events to journaled spend, so a fenced activation's stragglers are
/// excluded at quiescence, when fencing is known).
#[derive(Default)]
pub struct RunDiscipline {
    started: BTreeSet<(SessionId, TurnId)>,
    ended: BTreeSet<(SessionId, TurnId)>,
    /// (session, node) → activation ordinal, to attribute completions.
    activation: BTreeMap<(SessionId, NodeId), u64>,
    /// Activations that lost the fence: their speculative completions are
    /// excluded (§10.4).
    fenced: BTreeSet<(SessionId, NodeId, u64)>,
    /// Completions observed after their run ended, with the activation they
    /// belong to — judged at quiescence.
    suspects: Vec<(SessionId, TurnId, NodeId, u64)>,
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
            HarnessEvent::SessionActivated { session, node } => {
                *self.activation.entry((session.clone(), *node)).or_insert(0) += 1;
            }
            HarnessEvent::AppendRejected { session, node } => {
                let ordinal = self
                    .activation
                    .get(&(session.clone(), *node))
                    .copied()
                    .unwrap_or(0);
                self.fenced.insert((session.clone(), *node, ordinal));
            }
            // The guard's insert is the membership test and the recording in
            // one draw; no other arm matches `RunStarted`, so a false guard
            // (a fresh turn) simply falls through to the catch-all.
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
            HarnessEvent::ModelCompleted {
                session,
                turn,
                node,
                ..
            } if self.ended.contains(&(session.clone(), turn.clone())) => {
                let ordinal = self
                    .activation
                    .get(&(session.clone(), *node))
                    .copied()
                    .unwrap_or(0);
                self.suspects
                    .push((session.clone(), turn.clone(), *node, ordinal));
            }
            _ => {}
        }
        Ok(())
    }

    fn at_quiescence(&mut self) -> Result<(), String> {
        for (session, turn, node, ordinal) in &self.suspects {
            if !self.fenced.contains(&(session.clone(), *node, *ordinal)) {
                return Err(format!(
                    "model call for {session}/{turn} completed on {node} after the run ended, \
                     by an activation that was never fenced (H4/H5)"
                ));
            }
        }
        Ok(())
    }
}

/// The default checker set for harness workloads: the core invariants plus
/// the harness's continuous H-checkers (§11).
pub fn harness_invariants() -> Vec<Box<dyn Invariant>> {
    let mut invariants = default_invariants();
    invariants.push(Box::new(HarnessEventGrammar::default()));
    invariants.push(Box::new(RunDiscipline::default()));
    invariants
}

// ---------------------------------------------------------------------------
// The H catalogue (§11)
// ---------------------------------------------------------------------------

/// The harness invariant catalogue, H1–H8 (harness spec §11): machine
/// readable alongside the conformance suite, guarded by the drift test in
/// `conformance_catalogue.rs`. `invariant: n` reads as "Hn".
pub fn harness_catalogue() -> &'static [CatalogueEntry] {
    HARNESS_CATALOGUE
}

const HARNESS_CATALOGUE: &[CatalogueEntry] = &[
    CatalogueEntry {
        invariant: 1,
        spec: "harness §6.3, §7.5",
        property: "Deterministic fold and resume: state is a pure fold of the journal; a session resumed from any committed prefix behaves identically to one that never stopped, given the same subsequent outcomes",
        verify: &[Verify::SimTest(
            "harness/tests/conformance_resume.rs (differential), harness/tests/reproducibility.rs (seed sweep)",
        )],
    },
    CatalogueEntry {
        invariant: 2,
        spec: "harness §6.2",
        property: "Fenced single writer: committed records form one total order per session; a stale activation deactivates with no further appends, model calls, or tool calls",
        verify: &[
            Verify::Checker("harness-event-grammar"),
            Verify::SimTest("harness/tests/conformance_fence.rs (journal audit at quiescence)"),
        ],
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
        property: "Single activation per converged view: per-node activations never overlap; a converged healed cluster runs at most one activation per session; an owned session activates within bounded logical time of contact",
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
