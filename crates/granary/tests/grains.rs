//! End-to-end grain tests under deterministic simulation (granary §14).
//!
//! Mirrors the actor framework's V&V doctrine: drive grains through the public
//! API only, assert the grain invariants over the §13 event stream, decide a
//! recorded history against a reference model, and check seed-reproducibility.
//! Single-node Tier-1 covers G2, G3, G5, G6, and the G12 hibernation round-trip;
//! the cluster-only invariants (G1-under-election, G11, G13–G15) arrive with
//! Tier 2.

use std::collections::BTreeMap;
use std::collections::BTreeSet;
use std::sync::Arc;
use std::time::Duration;

use actor_core::BoxFuture;
use actor_core::Entropy;
use actor_core::Event;
use actor_core::EventSink;
use actor_core::LocalSystemBuilder;
use actor_core::Manifest;
use actor_core::Message;
use actor_simulation::Counter;
use actor_simulation::CounterOp;
use actor_simulation::CounterRet;
use actor_simulation::History;
use actor_simulation::Invariant;
use actor_simulation::Recorder;
use actor_simulation::SimEntropy;
use actor_simulation::SimSystem;
use actor_simulation::Simulation;
use actor_simulation::Workload;
use actor_simulation::check_linearizable;
use actor_simulation::check_reproducible;
use actor_simulation::default_invariants;
use actor_simulation::run_seed;
use granary::Grain;
use granary::GrainCtx;
use granary::GrainEvent;
use granary::GrainHandler;
use granary::GrainName;
use granary::GrainRef;
use granary::GranaryConfig;
use granary::GranaryExt;
use serde::Deserialize;
use serde::Serialize;

// --- A counter grain matching the linearizability `Counter` model -------------

#[derive(Default)]
struct CounterGrain;

#[derive(Default, Serialize, Deserialize)]
struct CounterState {
    value: i64,
}

#[derive(Serialize, Deserialize)]
enum CounterEvent {
    Added(i64),
}

impl Grain for CounterGrain {
    type System = SimSystem;
    type State = CounterState;
    type Event = CounterEvent;
    const GRAIN_TYPE: &'static str = "test.Counter";

    fn apply(state: &mut CounterState, event: &CounterEvent) {
        match event {
            CounterEvent::Added(d) => state.value += *d,
        }
    }

    fn register(r: &mut granary::GrainRegistry<Self>) {
        r.accept::<Add>();
        r.accept::<ReadCount>();
    }
}

#[derive(Clone, Serialize, Deserialize)]
struct Add(i64);
impl Message for Add {
    type Reply = i64; // the post-command value
    const MANIFEST: Manifest = Manifest::new("test.Add");
}

impl GrainHandler<Add> for CounterGrain {
    async fn handle(
        &self,
        state: &CounterState,
        msg: Add,
        _ctx: &GrainCtx<Self>,
    ) -> (Vec<CounterEvent>, i64) {
        // Non-idempotent: a double-fold shows up as a wrong Read, which the
        // linearizability checker catches (G2).
        (vec![CounterEvent::Added(msg.0)], state.value + msg.0)
    }
}

#[derive(Clone, Serialize, Deserialize)]
struct ReadCount;
impl Message for ReadCount {
    type Reply = i64;
    const MANIFEST: Manifest = Manifest::new("test.ReadCount");
}

impl GrainHandler<ReadCount> for CounterGrain {
    async fn handle(
        &self,
        state: &CounterState,
        _msg: ReadCount,
        _ctx: &GrainCtx<Self>,
    ) -> (Vec<CounterEvent>, i64) {
        (vec![], state.value) // read path: no events, commits nothing (§7.5)
    }
}

// --- Grain invariant checkers over the §13 event stream -----------------------

/// **Exactly-once activation per node** (invariant **G6**): a grain is never live
/// twice at once on the same node. The grain analogue of
/// `SingletonAtMostOnePerNode`; keyed by `(node, name)` so a grain that migrates
/// on failover (a fresh `Activated` on a *different* node) is not mistaken for a
/// second concurrent activation. Sound under the no-fault runs here (a host that
/// hibernates emits `Passivated` before its successor's `Activated`).
#[derive(Default)]
struct ExactlyOnceActivation {
    live: BTreeSet<(actor_core::NodeId, GrainName)>,
}

impl Invariant for ExactlyOnceActivation {
    fn name(&self) -> &'static str {
        "grain-exactly-once-activation"
    }

    fn observe(&mut self, event: &Event) -> Result<(), String> {
        let Some(grain_event) = event.as_app::<GrainEvent>() else {
            return Ok(());
        };
        match grain_event {
            GrainEvent::Activated { node, name } => {
                let fresh = self.live.insert((*node, name.clone()));
                if !fresh {
                    return Err(format!("grain {name} activated while already live on {node} (G6)"));
                }
            }
            GrainEvent::Passivated { node, name } => {
                self.live.remove(&(*node, name.clone()));
            }
            _ => {}
        }
        Ok(())
    }
}

/// **Commit head is monotonic** (invariants **G3**, **G5**): a grain's committed
/// seq strictly increases and never regresses — including across a hibernation
/// round-trip, since the head is rebuilt from the journal, not memory.
#[derive(Default)]
struct CommitMonotonic {
    last: BTreeMap<GrainName, u64>,
}

impl Invariant for CommitMonotonic {
    fn name(&self) -> &'static str {
        "grain-commit-monotonic"
    }

    fn observe(&mut self, event: &Event) -> Result<(), String> {
        if let Some(GrainEvent::Committed { name, seq, .. }) = event.as_app::<GrainEvent>() {
            let prev = self.last.get(name).copied().unwrap_or(0);
            if *seq <= prev {
                return Err(format!(
                    "grain {name} committed seq {seq} not after previous head {prev} (G3/G5)"
                ));
            }
            self.last.insert(name.clone(), *seq);
        }
        Ok(())
    }
}

fn grain_invariants() -> Vec<Box<dyn Invariant>> {
    let mut invariants = default_invariants();
    invariants.push(Box::new(ExactlyOnceActivation::default()));
    invariants.push(Box::new(CommitMonotonic::default()));
    invariants
}

// --- Linearizability workload (G2) --------------------------------------------

async fn counter_client(
    counter: GrainRef<CounterGrain>,
    history: History<Counter>,
    entropy: SimEntropy,
    ops: u64,
) {
    for _ in 0..ops {
        if entropy.next_u64() % 2 == 0 {
            let delta = 1 + (entropy.next_u64() % 3) as i64;
            let id = history.invoke(CounterOp::Add(delta));
            match counter.ask(Add(delta)).await {
                Ok(_value) => history.ok(id, CounterRet::AddOk),
                Err(_) => history.info(id),
            }
        } else {
            let id = history.invoke(CounterOp::Read);
            match counter.ask(ReadCount).await {
                Ok(value) => history.ok(id, CounterRet::Read(value)),
                Err(_) => history.info(id),
            }
        }
    }
}

struct CounterWorkload {
    clients: usize,
    ops: u64,
}

impl Workload for CounterWorkload {
    fn name(&self) -> &'static str {
        "linearizable-counter-grain"
    }

    fn run(&self, system: SimSystem) -> BoxFuture<'static, ()> {
        let clients = self.clients;
        let ops = self.ops;
        Box::pin(async move {
            let counters = system.granary::<CounterGrain>(GranaryConfig::default());
            // One grain, hammered concurrently — the single linearizable object.
            let counter = counters.grain("counter/0");
            let history: History<Counter> = History::new();
            let mut tasks = Vec::new();
            for _ in 0..clients {
                tasks.push(counter_client(
                    counter.clone(),
                    history.clone(),
                    system.entropy().clone(),
                    ops,
                ));
            }
            futures::future::join_all(tasks).await;

            let verdict = check_linearizable(&history);
            assert!(
                verdict.is_ok(),
                "counter grain history was not linearizable: {verdict:?}",
            );
        })
    }

    fn invariants(&self) -> Vec<Box<dyn Invariant>> {
        grain_invariants()
    }
}

#[test]
fn register_populates_the_command_allowlist() {
    // `Grain::register` fills the §5.5 deserialization allowlist.
    let accepted = granary::accepted_manifests::<CounterGrain>();
    assert!(accepted.contains("test.Add"));
    assert!(accepted.contains("test.ReadCount"));
    assert_eq!(accepted.len(), 2);
}

#[test]
fn counter_grain_is_linearizable_across_seeds() {
    let workload = CounterWorkload {
        clients: 4,
        ops: 8,
    };
    for seed in 0..96 {
        if let Err(failure) = run_seed(&workload, seed) {
            panic!("{failure}");
        }
    }
}

#[test]
fn counter_grain_run_is_reproducible() {
    // The determinism contract (§14): the same seed yields a byte-identical event
    // stream — and grain `App` events are part of it, so this guards G2 too.
    let workload = CounterWorkload {
        clients: 3,
        ops: 6,
    };
    for seed in 0..64 {
        if let Err(divergence) = check_reproducible(&workload, seed) {
            panic!("{divergence}");
        }
    }
}

// --- A deliberately broken grain, to prove the check has teeth ----------------

#[derive(Default)]
struct BuggyCounter;

impl Grain for BuggyCounter {
    type System = SimSystem;
    type State = CounterState;
    type Event = CounterEvent;
    const GRAIN_TYPE: &'static str = "test.BuggyCounter";

    fn apply(state: &mut CounterState, event: &CounterEvent) {
        match event {
            CounterEvent::Added(d) => state.value += *d,
        }
    }

    fn register(r: &mut granary::GrainRegistry<Self>) {
        r.accept::<Add>();
        r.accept::<ReadCount>();
    }
}

impl GrainHandler<Add> for BuggyCounter {
    async fn handle(
        &self,
        state: &CounterState,
        msg: Add,
        _ctx: &GrainCtx<Self>,
    ) -> (Vec<CounterEvent>, i64) {
        (vec![CounterEvent::Added(msg.0)], state.value + msg.0)
    }
}

impl GrainHandler<ReadCount> for BuggyCounter {
    async fn handle(
        &self,
        _state: &CounterState,
        _msg: ReadCount,
        _ctx: &GrainCtx<Self>,
    ) -> (Vec<CounterEvent>, i64) {
        (vec![], 999) // a value never written — any history with it is illegal
    }
}

#[test]
fn the_checker_catches_a_non_linearizable_grain() {
    let sim = Simulation::new(0);
    let system = LocalSystemBuilder::new(sim.clock(), sim.entropy(), sim.spawner()).build();
    let history: History<Counter> = History::new();
    let recorded = history.clone();
    sim.block_on(async move {
        let counters = system.granary::<BuggyCounter>(GranaryConfig::default());
        let counter = counters.grain("counter/0");
        let _ = counter.ask(Add(1)).await;
        let id = recorded.invoke(CounterOp::Read);
        let value = counter.ask(ReadCount).await.expect("local ask succeeds");
        recorded.ok(id, CounterRet::Read(value));
    });
    assert!(
        !check_linearizable(&history).is_ok(),
        "a grain returning a never-written value must be flagged non-linearizable",
    );
}

// --- Hibernation round-trip (G12) ---------------------------------------------

#[test]
fn hibernated_grain_reactivates_with_state() {
    let sim = Simulation::new(7);
    let recorder = Recorder::new();
    let sink: Arc<dyn EventSink> = Arc::new(recorder.clone());
    let system = LocalSystemBuilder::new(sim.clock(), sim.entropy(), sim.spawner())
        .events(sink)
        .build();
    // Aggressive idle window and frequent snapshots: a few adds cross the
    // snapshot threshold and the grain hibernates almost immediately after.
    let counters = system.granary::<CounterGrain>(GranaryConfig {
        idle_after: Duration::from_millis(10),
        snapshot_every: 4,
        ..GranaryConfig::default()
    });

    let counter = counters.grain("counter/0");
    let added = sim.block_on(async move {
        let mut total = 0;
        for _ in 0..10 {
            total = counter.ask(Add(1)).await.expect("add commits");
        }
        total
    });
    assert_eq!(added, 10);

    // Drive past the idle window: the grain snapshots, passivates, and stops.
    sim.run();
    let events = recorder.events();
    assert!(
        events
            .iter()
            .any(|e| matches!(e.as_app::<GrainEvent>(), Some(GrainEvent::Passivated { .. }))),
        "the idle grain must hibernate",
    );
    assert!(
        events
            .iter()
            .any(|e| matches!(e.as_app::<GrainEvent>(), Some(GrainEvent::Snapshotted { .. }))),
        "snapshots must bound the next replay",
    );

    // A fresh ref reactivates the name: it rehydrates from the journal and the
    // acknowledged writes survive (G12).
    let reread = counters.grain("counter/0");
    let value = sim.block_on(async move { reread.ask(ReadCount).await.expect("read after rehydrate") });
    assert_eq!(value, 10, "hibernation must not lose acknowledged writes");

    // The second activation rehydrated from the snapshot, not a full replay.
    let rehydrations: Vec<_> = recorder
        .events()
        .into_iter()
        .filter_map(|e| match e.as_app::<GrainEvent>() {
            Some(GrainEvent::Rehydrated { from_snapshot, .. }) => Some(*from_snapshot),
            _ => None,
        })
        .collect();
    assert_eq!(rehydrations, vec![false, true], "reactivation seeds from the snapshot");
}

// --- can_passivate: a grain that vetoes idle hibernation (§10) ----------------

/// A grain that never permits idle hibernation. The agentic harness overrides
/// `can_passivate` this way while a run is live (harness §7.2).
#[derive(Default)]
struct PinnedGrain;

impl Grain for PinnedGrain {
    type System = SimSystem;
    type State = CounterState;
    type Event = CounterEvent;
    const GRAIN_TYPE: &'static str = "test.Pinned";

    fn apply(state: &mut CounterState, event: &CounterEvent) {
        match event {
            CounterEvent::Added(d) => state.value += *d,
        }
    }

    fn register(r: &mut granary::GrainRegistry<Self>) {
        r.accept::<Add>();
    }

    fn can_passivate(&self, _state: &CounterState) -> bool {
        false
    }
}

impl GrainHandler<Add> for PinnedGrain {
    async fn handle(&self, state: &CounterState, msg: Add, _ctx: &GrainCtx<Self>) -> (Vec<CounterEvent>, i64) {
        (vec![CounterEvent::Added(msg.0)], state.value + msg.0)
    }
}

#[test]
fn can_passivate_vetoes_idle_hibernation() {
    use actor_core::Spawner;

    let sim = Simulation::new(9);
    let recorder = Recorder::new();
    let sink: Arc<dyn EventSink> = Arc::new(recorder.clone());
    let system = LocalSystemBuilder::new(sim.clock(), sim.entropy(), sim.spawner())
        .events(sink)
        .build();
    let pinned = system.granary::<PinnedGrain>(GranaryConfig {
        idle_after: Duration::from_millis(10),
        ..GranaryConfig::default()
    });

    // Drive with `run_for`, not `block_on`: a vetoing grain reschedules its idle
    // check forever, so the sim never quiesces — a bounded run drives the
    // activation and then well past many idle windows.
    sim.spawner().launch(Box::pin(async move {
        let _ = pinned.grain("p/0").ask(Add(1)).await;
    }));
    sim.run_for(Duration::from_secs(1));

    let events = recorder.events();
    assert!(
        events.iter().any(|e| matches!(e.as_app::<GrainEvent>(), Some(GrainEvent::Activated { .. }))),
        "the grain activated",
    );
    assert!(
        !events.iter().any(|e| matches!(e.as_app::<GrainEvent>(), Some(GrainEvent::Passivated { .. }))),
        "can_passivate = false must veto idle hibernation (§10)",
    );
}

// --- An account grain: the Appendix A end-to-end example ----------------------

#[derive(Default)]
struct Account;

#[derive(Default, Serialize, Deserialize)]
struct Balance {
    cents: i64,
}

#[derive(Serialize, Deserialize)]
enum Ledger {
    Deposited(u64),
    Withdrew(u64),
}

/// An application error — lives inside `M::Reply`, never in `GrainError` (§4.2).
#[derive(Debug, PartialEq, Eq, Serialize, Deserialize)]
struct Overdraft;

impl Grain for Account {
    type System = SimSystem;
    type State = Balance;
    type Event = Ledger;
    const GRAIN_TYPE: &'static str = "bank.Account";

    fn apply(state: &mut Balance, event: &Ledger) {
        match event {
            Ledger::Deposited(n) => state.cents += *n as i64,
            Ledger::Withdrew(n) => state.cents -= *n as i64,
        }
    }

    fn register(r: &mut granary::GrainRegistry<Self>) {
        r.accept::<Withdraw>();
        r.accept::<Deposit>();
        r.accept::<ReadBalance>();
    }
}

#[derive(Clone, Serialize, Deserialize)]
struct Withdraw {
    cents: u64,
}
impl Message for Withdraw {
    type Reply = Result<i64, Overdraft>;
    const MANIFEST: Manifest = Manifest::new("bank.Withdraw");
}

impl GrainHandler<Withdraw> for Account {
    async fn handle(
        &self,
        state: &Balance,
        msg: Withdraw,
        _ctx: &GrainCtx<Self>,
    ) -> (Vec<Ledger>, Result<i64, Overdraft>) {
        if (state.cents as u64) < msg.cents {
            return (vec![], Err(Overdraft)); // no event, nothing to commit
        }
        (
            vec![Ledger::Withdrew(msg.cents)],
            Ok(state.cents - msg.cents as i64),
        )
    }
}

#[derive(Clone, Serialize, Deserialize)]
struct Deposit {
    cents: u64,
}
impl Message for Deposit {
    type Reply = i64;
    const MANIFEST: Manifest = Manifest::new("bank.Deposit");
}

impl GrainHandler<Deposit> for Account {
    async fn handle(
        &self,
        state: &Balance,
        msg: Deposit,
        _ctx: &GrainCtx<Self>,
    ) -> (Vec<Ledger>, i64) {
        (vec![Ledger::Deposited(msg.cents)], state.cents + msg.cents as i64)
    }
}

#[derive(Clone, Serialize, Deserialize)]
struct ReadBalance;
impl Message for ReadBalance {
    type Reply = i64;
    const MANIFEST: Manifest = Manifest::new("bank.ReadBalance");
}

impl GrainHandler<ReadBalance> for Account {
    async fn handle(
        &self,
        state: &Balance,
        _msg: ReadBalance,
        _ctx: &GrainCtx<Self>,
    ) -> (Vec<Ledger>, i64) {
        (vec![], state.cents)
    }
}

#[test]
fn account_grain_end_to_end() {
    let sim = Simulation::new(3);
    let system = LocalSystemBuilder::new(sim.clock(), sim.entropy(), sim.spawner()).build();
    let accounts = system.granary::<Account>(GranaryConfig::default());

    let acct = accounts.grain("account/42");
    sim.block_on(async move {
        // Committed + durable: the reply reflects post-command state.
        assert_eq!(acct.ask(Deposit { cents: 1000 }).await.unwrap(), 1000);
        assert_eq!(acct.ask(Withdraw { cents: 500 }).await.unwrap(), Ok(500));
        // Application outcome — an error value inside the reply, not a GrainError.
        assert_eq!(acct.ask(Withdraw { cents: 9999 }).await.unwrap(), Err(Overdraft));
        // The overdrawn withdraw committed nothing (§7.5): balance is unchanged.
        assert_eq!(acct.ask(ReadBalance).await.unwrap(), 500);
    });
}

#[test]
fn tell_commits_fire_and_forget() {
    let sim = Simulation::new(11);
    let system = LocalSystemBuilder::new(sim.clock(), sim.entropy(), sim.spawner()).build();
    let accounts = system.granary::<Account>(GranaryConfig::default());

    let acct = accounts.grain("account/99");
    let balance = sim.block_on(async move {
        // `tell` returns once the host accepts the command, not after the commit.
        acct.tell(Deposit { cents: 300 }).await.expect("tell enqueues");
        // The follow-up `ask` reaches the same host behind the telled deposit
        // (FIFO mailbox), so it observes the committed effect.
        acct.ask(ReadBalance).await.unwrap()
    });
    assert_eq!(balance, 300);
}

#[test]
fn cached_host_is_reused_and_self_heals_after_hibernation() {
    // §5.4 host-ref caching: the first call resolves through the gateway and caches
    // the host; a later call hits the cache and skips the gateway. When the cached
    // host hibernates (§10) the cached handle goes stale — the next call's `ask`
    // dead-letters, the cache is invalidated, and the command is re-issued through a
    // fresh activation, transparently (the command did not run, so the re-issue is
    // safe — at-most-once is preserved).
    let sim = Simulation::new(21);
    let system = LocalSystemBuilder::new(sim.clock(), sim.entropy(), sim.spawner()).build();
    let accounts = system.granary::<Account>(GranaryConfig {
        idle_after: Duration::from_millis(10),
        ..GranaryConfig::default()
    });

    let cached_after_first = sim.block_on({
        let accounts = accounts.clone();
        async move {
            let _ = accounts.grain("account/1").ask(Deposit { cents: 1000 }).await.unwrap();
            // The first call populated the cache: a second call now bypasses the gateway.
            accounts.is_cached("account/1")
        }
    });
    assert!(cached_after_first, "the first call must cache the resolved host (§5.4)");

    // Drive past the idle window so the cached host hibernates and stops.
    sim.run();

    // A call through the same (now-stale) cache transparently re-activates and reads
    // the durable balance — the stale entry is invalidated and the command re-issued.
    let balance = sim.block_on({
        let accounts = accounts.clone();
        async move { accounts.grain("account/1").ask(ReadBalance).await.unwrap() }
    });
    assert_eq!(balance, 1000, "a stale cached host must self-heal, losing no acknowledged write");
}

#[test]
fn account_balance_survives_reactivation() {
    let sim = Simulation::new(5);
    let system = LocalSystemBuilder::new(sim.clock(), sim.entropy(), sim.spawner()).build();
    let accounts = system.granary::<Account>(GranaryConfig {
        idle_after: Duration::from_millis(10),
        ..GranaryConfig::default()
    });

    let acct = accounts.grain("account/7");
    sim.block_on(async move {
        let _ = acct.ask(Deposit { cents: 250 }).await.unwrap();
    });
    // Hibernate, then read through a fresh activation.
    sim.run();
    let reread = accounts.grain("account/7");
    let balance = sim.block_on(async move { reread.ask(ReadBalance).await.unwrap() });
    assert_eq!(balance, 250, "a durable deposit must survive hibernation (G12)");
}

#[test]
fn granary_named_hosts_one_grain_under_many_type_names() {
    // The extension point the agentic harness rides (§5.1): one Rust grain (its
    // run loop) hosted under many runtime type names — one per kind. Each name is
    // its own grain type (its own gateway key, shard map, and namespace), so the
    // same key under two names addresses two **independent** grains. The factory
    // also replaces `G::default`, the seam the harness uses to inject per-node
    // handles into each fresh activation.
    let sim = Simulation::new(5);
    let system = LocalSystemBuilder::new(sim.clock(), sim.entropy(), sim.spawner()).build();

    let researchers = system.granary_named::<CounterGrain>(
        "harness.researcher",
        GranaryConfig::default(),
        Arc::new(CounterGrain::default),
    );
    let summarizers = system.granary_named::<CounterGrain>(
        "harness.summarizer",
        GranaryConfig::default(),
        Arc::new(CounterGrain::default),
    );

    // The runtime type name lands in the `GrainName`, overriding `G::GRAIN_TYPE`.
    assert_eq!(researchers.grain("c/0").name().grain_type(), "harness.researcher");
    assert_eq!(summarizers.grain("c/0").name().grain_type(), "harness.summarizer");

    sim.block_on(async move {
        // Same key under two type names → two independent grains, no crosstalk.
        assert_eq!(researchers.grain("c/0").ask(Add(3)).await.unwrap(), 3);
        assert_eq!(summarizers.grain("c/0").ask(Add(10)).await.unwrap(), 10);
        assert_eq!(researchers.grain("c/0").ask(ReadCount).await.unwrap(), 3);
        assert_eq!(summarizers.grain("c/0").ask(ReadCount).await.unwrap(), 10);
    });
}
