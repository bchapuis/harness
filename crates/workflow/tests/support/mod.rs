//! A workflow entity and a scripted effect, shared by the conformance suites.
//!
//! `Fanout` is the workflow spec's Appendix A worked example, made real: an entity
//! that fans a job out to N workers, collects their results, and finishes. It
//! journals its own records, holds no memo, and writes no dispatch loop, no
//! in-flight table, no "did I already launch this", no recovery pass, no
//! straggler check, and no `can_hibernate` body of its own.
//!
//! `Script` is the workflow spec §10 simulation seam: the one row the workflow adds to
//! the table the entity already virtualizes. A scripted effect is a deterministic
//! function of the key and the seed, with seeded latency, failure, and
//! never-returning ("crash mid-effect") outcomes.

#![allow(dead_code)] // each suite uses a different part of the surface

pub mod catalogue;
pub mod cluster;

use std::collections::BTreeMap;
use std::collections::BTreeSet;
use std::sync::Arc;
use std::sync::Mutex;
use std::time::Duration;

use actor_core::Event;
use actor_core::EventSink;
use actor_core::LocalSystemBuilder;
use actor_core::Manifest;
use actor_core::Message;
use actor_core::NodeId;
use actor_simulation::Checker;
use actor_simulation::Recorder;
use actor_simulation::SimSystem;
use actor_simulation::Simulation;
use entity::Entities;
use entity::Entity;
use entity::EntityConfig;
use entity::EntityCtx;
use entity::EntityEvent;
use entity::EntityExt;
use entity::EntityHandler;
use entity::EntityRegistry;
use entity::EntitySystem;
use serde::Deserialize;
use serde::Serialize;
use workflow::EffectError;
use workflow::Engine;
use workflow::OnOrphan;
use workflow::Pending;
use workflow::Perform;
use workflow::Settled;
use workflow::Tick;
use workflow::Workflow;
use workflow::WorkflowEvent;
use workflow::testing::WorkflowCycle;

pub type TaskId = u32;

/// The entity type every suite hosts `Fanout` under.
pub const FANOUT_TYPE: &str = "test.Fanout";

/// What a scripted effect does when its key is performed.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Outcome {
    /// Answer after `delay` of virtual time.
    Answer { delay: Duration },
    /// Fail after `delay`, with an error the consumer folds as a value (§4.3).
    Fail { delay: Duration },
    /// Never return: the effect is in flight forever, which is how a suite holds
    /// a claim open across a hibernation attempt or a step-down.
    Hang,
    /// Panic inside `perform`, which the workflow catches at the task boundary and
    /// settles as `Failed` (§4.3).
    Panic,
}

impl Default for Outcome {
    fn default() -> Self {
        Outcome::Answer {
            delay: Duration::ZERO,
        }
    }
}

/// The scripted effect seam (§10): what each key does, and a ledger of what was
/// actually performed, so a suite can assert on launches the event stream would
/// only show indirectly. A key with no script answers at once.
#[derive(Default)]
pub struct Script {
    per_key: BTreeMap<TaskId, Outcome>,
    /// Every key performed, in order. Length per key is the number of times the
    /// effect really ran, which is what W3 is about.
    performed: Vec<TaskId>,
    /// Every key whose effect *returned*, in order. The pair with `performed` is
    /// what separates "launched twice" from "two outcomes in flight", which is
    /// the difference a straggler test turns on.
    completed: Vec<TaskId>,
}

impl Script {
    pub fn set(&mut self, key: TaskId, outcome: Outcome) {
        self.per_key.insert(key, outcome);
    }

    fn outcome(&self, key: TaskId) -> Outcome {
        self.per_key.get(&key).cloned().unwrap_or_default()
    }

    /// Forget every scripted outcome and every ledger entry: a sweep runs one
    /// seed after another through one seam, and each run's ledger must be a
    /// function of its seed alone or a reproducibility replay would disagree with
    /// itself.
    pub fn reset(&mut self) {
        *self = Script::default();
    }

    /// How many times `key`'s effect actually ran.
    pub fn runs(&self, key: TaskId) -> usize {
        self.performed.iter().filter(|k| **k == key).count()
    }

    pub fn total_runs(&self) -> usize {
        self.performed.len()
    }

    /// How many keys ran more than once, across activations, since one
    /// activation launches a key at most once per claim (W3).
    pub fn relaunched(&self) -> usize {
        let mut runs: BTreeMap<TaskId, usize> = BTreeMap::new();
        for key in &self.performed {
            *runs.entry(*key).or_default() += 1;
        }
        runs.values().filter(|n| **n > 1).count()
    }

    /// How many of `key`'s launches have returned an outcome.
    pub fn completions(&self, key: TaskId) -> usize {
        self.completed.iter().filter(|k| **k == key).count()
    }
}

/// A handle to the script, cloned into `perform`'s `'static` future the way a
/// consumer clones its real seams out of `&self` (§4.1).
pub type Seam = Arc<Mutex<Script>>;

fn lock(seam: &Seam) -> std::sync::MutexGuard<'_, Script> {
    seam.lock().unwrap_or_else(|e| e.into_inner())
}

// --- The entity: ordinary entity. Records, a fold, handlers (entity §4.1, §4.2).

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Task {
    pub id: TaskId,
    /// The recovery policy this task's effect declares (§4.4). Journaled with
    /// the task, because `pending` must re-derive it identically on every
    /// activation and it is part of what the consumer decided.
    pub on_orphan: OnOrphan,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum Answer {
    Ok,
    Failed(String),
}

#[derive(Default, Serialize, Deserialize)]
pub struct FanoutState {
    pub requested: Vec<Task>,
    pub done: BTreeMap<TaskId, Answer>,
    pub finished: bool,
    /// Keys whose settle the consumer deliberately journals nothing for, so the
    /// suites can exercise the record-less settle path (§4.2) and the backoff
    /// entry point it depends on (§3.2 item 3).
    pub silent: BTreeSet<TaskId>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum FanoutRecord {
    Requested {
        tasks: Vec<Task>,
        silent: Vec<TaskId>,
    },
    Answered {
        task: TaskId,
        answer: Answer,
    },
    Finished,
    /// Retire a task without answering it: the record that takes a key *out* of
    /// the desired set with its effect still in flight, which is how the suites
    /// produce a straggler (§4.5).
    Retracted {
        task: TaskId,
    },
}

pub struct Fanout<S: EntitySystem> {
    pub seam: Seam,
    pub engine: Engine<Fanout<S>>,
}

impl<S: EntitySystem> Fanout<S> {
    pub fn new(seam: Seam) -> Self {
        Fanout {
            seam,
            engine: Engine::default(),
        }
    }
}

impl<S: EntitySystem> Entity for Fanout<S> {
    type System = S;
    type State = FanoutState;
    type Event = FanoutRecord;
    type Facets = ();
    const ENTITY_TYPE: &'static str = "test.Fanout";

    fn apply(state: &mut FanoutState, record: &FanoutRecord) {
        match record {
            // Idempotent by task id (entity §2.2): a request re-asked after an
            // ambiguous failure (a timeout, a leader move mid-append) may
            // commit twice, and the second must not double the work.
            FanoutRecord::Requested { tasks, silent } => {
                for task in tasks {
                    if !state.requested.iter().any(|t| t.id == task.id) {
                        state.requested.push(task.clone());
                    }
                }
                state.silent.extend(silent.iter().copied());
            }
            FanoutRecord::Answered { task, answer } => {
                state.done.insert(*task, answer.clone());
            }
            FanoutRecord::Finished => state.finished = true,
            FanoutRecord::Retracted { task } => {
                state.requested.retain(|t| t.id != *task);
            }
        }
    }

    fn register(r: &mut EntityRegistry<Self>) {
        // The network allowlist (entity §5.5). The workflow's `Tick` and
        // `Settled` are local self-tells and are deliberately absent: nothing
        // outside the entity injects an effect outcome.
        r.accept::<Request>();
        r.accept::<Retract>();
        r.accept::<Status>();
    }

    async fn on_activate(&mut self, ctx: &EntityCtx<Self>) -> Result<(), actor_core::BoxError> {
        // Forget every claim and count, arm the first tick, and tick so a
        // resumed entity with work outstanding drives it without a caller (§4.4).
        self.engine.on_activate(ctx);
        Ok(())
    }

    fn can_hibernate(&self, _state: &FanoutState) -> bool {
        // Derived, not hand-written (§6). A consumer with work that no effect
        // represents would conjoin its own clause here; this one has none.
        self.engine.quiescent()
    }
}

// --- The workflow: what the committed state calls for, how to do one, what an
// --- outcome means.

impl<S: EntitySystem> Workflow for Fanout<S> {
    type Key = TaskId;
    type Effect = Task;
    type Outcome = Answer;

    /// Pure (§3.1). Every task requested and not yet answered, at once the
    /// dispatch list, the recovery set after a crash, and the straggler filter.
    fn pending(&self, state: &FanoutState) -> Vec<Pending<TaskId, Task>> {
        state
            .requested
            .iter()
            .filter(|t| !state.done.contains_key(&t.id))
            .map(|t| Pending {
                key: t.id,
                effect: t.clone(),
                on_orphan: t.on_orphan,
            })
            .collect()
    }

    /// The only I/O (§4.1). Off the command path, holding nothing: the seam is
    /// cloned out of `&self` before the future is returned, which is why the
    /// future is `'static`.
    fn perform(
        &self,
        task: Task,
        cx: Perform<S>,
    ) -> impl Future<Output = Result<Answer, EffectError>> + Send + 'static {
        let seam = Arc::clone(&self.seam);
        async move {
            let outcome = {
                let mut script = lock(&seam);
                script.performed.push(task.id);
                script.outcome(task.id)
            };
            match outcome {
                Outcome::Answer { delay } => {
                    cx.system().sleep(delay).await;
                    lock(&seam).completed.push(task.id);
                    Ok(Answer::Ok)
                }
                Outcome::Fail { delay } => {
                    cx.system().sleep(delay).await;
                    lock(&seam).completed.push(task.id);
                    Err(EffectError::Failed(format!("task {} failed", task.id)))
                }
                Outcome::Hang => {
                    std::future::pending::<()>().await;
                    unreachable!()
                }
                Outcome::Panic => panic!("task {} panicked", task.id),
            }
        }
    }

    /// A pure decision (§4.2), like any handler: records out, no I/O, no
    /// mutation.
    async fn settle(
        &self,
        state: &FanoutState,
        key: &TaskId,
        outcome: Result<Answer, EffectError>,
        _ctx: &EntityCtx<Self>,
    ) -> Vec<FanoutRecord> {
        if state.silent.contains(key) {
            // "This outcome changed nothing" (§4.2): no record, so nothing
            // commits and only the backoff timer brings the next tick.
            return Vec::new();
        }
        let answer = match outcome {
            Ok(a) => a,
            // A failure is a value (§4.3), not a fault.
            Err(e) => Answer::Failed(e.to_string()),
        };
        let mut out = vec![FanoutRecord::Answered { task: *key, answer }];
        if state.done.len() + 1 == state.requested.len() {
            out.push(FanoutRecord::Finished); // the last answer ends the work
        }
        out
    }
}

// --- The two handlers the workflow requires. `Workflow`'s supertraits make these a
// --- compile-time obligation rather than a documented one.

impl<S: EntitySystem> EntityHandler<Tick> for Fanout<S> {
    async fn handle(
        &self,
        state: &FanoutState,
        _msg: Tick,
        ctx: &EntityCtx<Self>,
    ) -> (Vec<FanoutRecord>, ()) {
        (self.engine.advance(self, state, ctx), ())
    }
}

impl<S: EntitySystem> EntityHandler<Settled<Fanout<S>>> for Fanout<S> {
    async fn handle(
        &self,
        state: &FanoutState,
        msg: Settled<Fanout<S>>,
        ctx: &EntityCtx<Self>,
    ) -> (Vec<FanoutRecord>, ()) {
        // The workflow re-enters the cycle itself after a committed settle, or arms
        // the backoff timer after one that committed nothing (§3.2 items 1, 3).
        (self.engine.settled(self, state, msg, ctx).await, ())
    }
}

// --- The consumer's own commands.

/// Request `tasks`, journaling the intent. The effects launch from the workflow
/// that follows the commit, never from this handler: intent before effect (§5)
/// is the cycle's shape, not a check.
#[derive(Clone, Serialize, Deserialize)]
pub struct Request {
    pub tasks: Vec<Task>,
    /// Keys whose settle will journal nothing (§4.2).
    pub silent: Vec<TaskId>,
}

impl Message for Request {
    type Reply = ();
    const MANIFEST: Manifest = Manifest::new("test.FanoutRequest");
}

impl<S: EntitySystem> EntityHandler<Request> for Fanout<S> {
    async fn handle(
        &self,
        state: &FanoutState,
        msg: Request,
        ctx: &EntityCtx<Self>,
    ) -> (Vec<FanoutRecord>, ()) {
        self.engine.tick(self, state, ctx);
        (
            vec![FanoutRecord::Requested {
                tasks: msg.tasks,
                silent: msg.silent,
            }],
            (),
        )
    }
}

/// Retract a task: it leaves the desired set with its effect possibly still in
/// flight, so its outcome becomes a straggler (§4.5).
#[derive(Clone, Serialize, Deserialize)]
pub struct Retract(pub TaskId);

impl Message for Retract {
    type Reply = ();
    const MANIFEST: Manifest = Manifest::new("test.FanoutRetract");
}

impl<S: EntitySystem> EntityHandler<Retract> for Fanout<S> {
    async fn handle(
        &self,
        state: &FanoutState,
        msg: Retract,
        ctx: &EntityCtx<Self>,
    ) -> (Vec<FanoutRecord>, ()) {
        self.engine.tick(self, state, ctx);
        (vec![FanoutRecord::Retracted { task: msg.0 }], ())
    }
}

/// A read: no records, commits nothing (entity §7.5).
#[derive(Clone, Serialize, Deserialize)]
pub struct Status;

/// `(answers, finished, outstanding)`.
pub type StatusReply = (BTreeMap<TaskId, Answer>, bool, Vec<TaskId>);

impl Message for Status {
    type Reply = StatusReply;
    const MANIFEST: Manifest = Manifest::new("test.FanoutStatus");
}

impl<S: EntitySystem> EntityHandler<Status> for Fanout<S> {
    async fn handle(
        &self,
        state: &FanoutState,
        _msg: Status,
        _ctx: &EntityCtx<Self>,
    ) -> (Vec<FanoutRecord>, StatusReply) {
        let outstanding = self.pending(state).into_iter().map(|p| p.key).collect();
        (vec![], (state.done.clone(), state.finished, outstanding))
    }
}

// --- Fixtures.

pub fn task(id: TaskId) -> Task {
    Task {
        id,
        on_orphan: OnOrphan::Reexecute,
    }
}

pub fn interrupting_task(id: TaskId) -> Task {
    Task {
        id,
        on_orphan: OnOrphan::Interrupt,
    }
}

/// A request whose every settle journals a record.
pub fn request(tasks: Vec<Task>) -> Request {
    Request {
        tasks,
        silent: vec![],
    }
}

pub fn sim_system(sim: &Simulation, recorder: &Recorder) -> SimSystem {
    let sink: Arc<dyn EventSink> = Arc::new(recorder.clone());
    LocalSystemBuilder::new(sim.clock(), sim.entropy(), sim.spawner())
        .events(sink)
        .build()
}

/// Host `Fanout` on the single-node system, every activation sharing `seam`.
pub fn fanouts(
    system: &SimSystem,
    seam: &Seam,
    config: EntityConfig,
) -> Entities<Fanout<SimSystem>> {
    let seam = seam.clone();
    system.entity_node().entities_named::<Fanout<SimSystem>>(
        FANOUT_TYPE,
        config,
        Arc::new(move || Fanout::new(seam.clone())),
    )
}

/// Ask `key`'s entity on the single-node tier and return its reply. `block_on`
/// returns at the reply; whatever the command set in motion runs on the next
/// `run_for`.
pub fn ask<M>(
    sim: &Simulation,
    fanouts: &Entities<Fanout<SimSystem>>,
    key: &str,
    msg: M,
) -> M::Reply
where
    M: Message + Clone + Send + 'static,
    M::Reply: Send + 'static,
    Fanout<SimSystem>: EntityHandler<M>,
{
    let entity = fanouts.entity(key);
    sim.block_on(async move { entity.ask(msg).await.expect("the entity replies") })
}

pub fn seam() -> Seam {
    Arc::new(Mutex::new(Script::default()))
}

pub fn with_script(seam: &Seam, f: impl FnOnce(&mut Script)) {
    f(&mut lock(seam));
}

pub fn read_script<T>(seam: &Seam, f: impl FnOnce(&Script) -> T) -> T {
    f(&lock(seam))
}

// --- Reading the stream. Each takes a snapshot of the recorder, so a test
// --- clones the log once per group of assertions rather than once per question.

/// The workflow's own events, in stream order.
pub fn drive_events(events: &[Event]) -> Vec<WorkflowEvent> {
    events
        .iter()
        .filter_map(Event::as_app::<WorkflowEvent>)
        .cloned()
        .collect()
}

fn keys(events: &[Event], select: impl Fn(&WorkflowEvent) -> Option<&str>) -> Vec<String> {
    events
        .iter()
        .filter_map(Event::as_app::<WorkflowEvent>)
        .filter_map(|e| select(e).map(str::to_string))
        .collect()
}

/// Every key launched, on any node.
pub fn launched(events: &[Event]) -> Vec<String> {
    keys(events, |e| match e {
        WorkflowEvent::Launched { key, .. } => Some(key),
        _ => None,
    })
}

/// Every key launched by `node`'s activation.
pub fn launched_on(events: &[Event], node: NodeId) -> Vec<String> {
    keys(events, |e| match e {
        WorkflowEvent::Launched { key, node: n, .. } if *n == node => Some(key),
        _ => None,
    })
}

/// Every orphaned key `node`'s activation resolved `Interrupted` (§4.4).
pub fn interrupted_on(events: &[Event], node: NodeId) -> Vec<String> {
    keys(events, |e| match e {
        WorkflowEvent::Interrupted { key, node: n, .. } if *n == node => Some(key),
        _ => None,
    })
}

/// Every key whose outcome was discarded as a straggler (§4.5).
pub fn discarded(events: &[Event]) -> Vec<String> {
    keys(events, |e| match e {
        WorkflowEvent::Discarded { key, .. } => Some(key),
        _ => None,
    })
}

/// Every settle, as `(key, committed)`.
pub fn settled(events: &[Event]) -> Vec<(String, bool)> {
    drive_events(events)
        .into_iter()
        .filter_map(|e| match e {
            WorkflowEvent::Settled { key, committed, .. } => Some((key, committed)),
            _ => None,
        })
        .collect()
}

/// Whether any activation announced quiescence (§6).
pub fn announced_quiescence(events: &[Event]) -> bool {
    events
        .iter()
        .filter_map(Event::as_app::<WorkflowEvent>)
        .any(|e| matches!(e, WorkflowEvent::Quiescent { .. }))
}

/// Whether any entity hibernated (entity §13).
pub fn hibernated(events: &[Event]) -> bool {
    events.iter().any(|e| {
        matches!(
            e.as_app::<EntityEvent>(),
            Some(EntityEvent::Deactivated { .. })
        )
    })
}

/// Hold the workflow's grammar over a recorded stream at the end of a clustered
/// scenario, through the same `Checker` the swarm runner uses: the failover
/// checker, because every such scenario deposes a leader with claims open, which
/// is legal (§6).
pub fn assert_grammar(events: Vec<Event>) {
    let checker = Checker::new(vec![Box::new(WorkflowCycle::under_failover())]);
    for event in events {
        checker.emit(event);
    }
    let violations = checker.finish();
    assert!(
        violations.is_empty(),
        "workflow-cycle violated: {violations:?}"
    );
}
