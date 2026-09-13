//! End-to-end entity tests under deterministic simulation (entity §14).
//!
//! Mirrors the actor framework's V&V doctrine: drive entities through the public
//! API only, assert the entity invariants over the §13 event stream, decide a
//! recorded history against a reference model, and check seed-reproducibility.
//! The single-node `Local` tier covers E2, E3, E5, E6, and the E12 hibernation
//! round-trip; the cluster-only invariants (E1-under-election, E11, E13–E15)
//! arrive with the `Quorum` tier.

use std::sync::Arc;
use std::time::Duration;

use actor_core::EventSink;
use actor_core::LocalSystemBuilder;
use actor_core::Manifest;
use actor_core::Message;
use actor_simulation::Counter;
use actor_simulation::CounterOp;
use actor_simulation::CounterRet;
use actor_simulation::History;
use actor_simulation::Recorder;
use actor_simulation::SimSystem;
use actor_simulation::Simulation;
use actor_simulation::check_linearizable;
mod support;

use entity::Entity;
use entity::EntityConfig;
use entity::EntityCtx;
use entity::EntityEvent;
use entity::EntityExt;
use entity::EntityHandler;
use serde::Deserialize;
use serde::Serialize;

use support::Add;
use support::CounterEntity;
use support::CounterEvent;
use support::CounterState;
use support::ReadCount;

#[test]
fn register_populates_the_command_allowlist() {
    // `Entity::register` fills the §5.5 deserialization allowlist.
    let accepted = entity::accepted_manifests::<CounterEntity>();
    assert!(accepted.contains("test.Add"));
    assert!(accepted.contains("test.ReadCount"));
    assert_eq!(accepted.len(), 2);
}

// --- A deliberately broken entity, to prove the check has teeth ----------------

#[derive(Default)]
struct BuggyCounter;

impl Entity for BuggyCounter {
    type System = SimSystem;
    type State = CounterState;
    type Event = CounterEvent;
    type Facets = ();
    const ENTITY_TYPE: &'static str = "test.BuggyCounter";

    fn apply(state: &mut CounterState, event: &CounterEvent) {
        match event {
            CounterEvent::Added(d) => state.value += *d,
        }
    }

    fn register(r: &mut entity::EntityRegistry<Self>) {
        r.accept::<Add>();
        r.accept::<ReadCount>();
    }
}

impl EntityHandler<Add> for BuggyCounter {
    async fn handle(
        &self,
        state: &CounterState,
        msg: Add,
        _ctx: &EntityCtx<Self>,
    ) -> (Vec<CounterEvent>, i64) {
        (vec![CounterEvent::Added(msg.0)], state.value + msg.0)
    }
}

impl EntityHandler<ReadCount> for BuggyCounter {
    async fn handle(
        &self,
        _state: &CounterState,
        _msg: ReadCount,
        _ctx: &EntityCtx<Self>,
    ) -> (Vec<CounterEvent>, i64) {
        (vec![], 999) // a value never written: any history with it is illegal
    }
}

#[test]
fn the_checker_catches_a_non_linearizable_entity() {
    let sim = Simulation::new(0);
    let system = LocalSystemBuilder::new(sim.clock(), sim.entropy(), sim.spawner()).build();
    let history: History<Counter> = History::new();
    let recorded = history.clone();
    sim.block_on(async move {
        let counters = system.entities::<BuggyCounter>(EntityConfig::default());
        let counter = counters.entity("counter/0");
        let _ = counter.ask(Add(1)).await;
        let id = recorded.invoke(CounterOp::Read);
        let value = counter.ask(ReadCount).await.expect("local ask succeeds");
        recorded.ok(id, CounterRet::Read(value));
    });
    assert!(
        !check_linearizable(&history).is_ok(),
        "an entity returning a never-written value must be flagged non-linearizable",
    );
}

// --- Hibernation round-trip (E12) ---------------------------------------------

#[test]
fn hibernated_entity_reactivates_with_state() {
    let sim = Simulation::new(7);
    let recorder = Recorder::new();
    let sink: Arc<dyn EventSink> = Arc::new(recorder.clone());
    let system = LocalSystemBuilder::new(sim.clock(), sim.entropy(), sim.spawner())
        .events(sink)
        .build();
    // Short idle window and frequent snapshots: a few adds cross the snapshot
    // threshold and the entity hibernates right after.
    let counters = system.entities::<CounterEntity>(EntityConfig {
        idle_after: Duration::from_millis(10),
        snapshot_every: 4,
        ..EntityConfig::default()
    });

    let counter = counters.entity("counter/0");
    let added = sim.block_on(async move {
        let mut total = 0;
        for _ in 0..10 {
            total = counter.ask(Add(1)).await.expect("add commits");
        }
        total
    });
    assert_eq!(added, 10);

    // Drive past the idle window: the entity snapshots, hibernates, and stops.
    sim.run();
    let events = recorder.events();
    assert!(
        events.iter().any(|e| matches!(
            e.as_app::<EntityEvent>(),
            Some(EntityEvent::Deactivated { .. })
        )),
        "the idle entity must hibernate",
    );
    assert!(
        events.iter().any(|e| matches!(
            e.as_app::<EntityEvent>(),
            Some(EntityEvent::Snapshotted { .. })
        )),
        "snapshots must bound the next replay",
    );

    // A fresh ref reactivates the name: it rehydrates from the journal and the
    // acknowledged writes survive (E12).
    let reread = counters.entity("counter/0");
    let value =
        sim.block_on(async move { reread.ask(ReadCount).await.expect("read after rehydrate") });
    assert_eq!(value, 10, "hibernation must not lose acknowledged writes");

    // The second activation rehydrated from the snapshot, not a full replay.
    let rehydrations: Vec<_> = recorder
        .events()
        .into_iter()
        .filter_map(|e| match e.as_app::<EntityEvent>() {
            Some(EntityEvent::Rehydrated { from_snapshot, .. }) => Some(*from_snapshot),
            _ => None,
        })
        .collect();
    assert_eq!(
        rehydrations,
        vec![false, true],
        "reactivation seeds from the snapshot"
    );
}

// --- The non-activating read: observation leaves hibernation alone (§7.5) -----

#[test]
fn a_gateway_read_leaves_a_hibernated_entity_asleep() {
    let sim = Simulation::new(11);
    let recorder = Recorder::new();
    let sink: Arc<dyn EventSink> = Arc::new(recorder.clone());
    let system = LocalSystemBuilder::new(sim.clock(), sim.entropy(), sim.spawner())
        .events(sink)
        .build();
    let counters = system.entities::<CounterEntity>(EntityConfig {
        idle_after: Duration::from_millis(10),
        ..EntityConfig::default()
    });

    let counter = counters.entity("counter/0");
    sim.block_on(async move {
        for _ in 0..3 {
            counter.ask(Add(1)).await.expect("add commits");
        }
    });

    // Drive past the idle window: the entity hibernates.
    sim.run();
    assert!(
        recorder.events().iter().any(|e| matches!(
            e.as_app::<EntityEvent>(),
            Some(EntityEvent::Deactivated { .. })
        )),
        "the idle entity must hibernate",
    );

    // The gateway serves the committed events from the journal (§7.5): correct
    // content, ascending real slots, short only at the head.
    let reread = counters.entity("counter/0");
    let events = sim.block_on(async move {
        reread
            .events(entity::Seq::ZERO, 100)
            .await
            .expect("a read after hibernation succeeds")
    });
    assert_eq!(events.events.len(), 3, "all committed events come back");
    assert!(
        events
            .events
            .iter()
            .all(|(_, e)| matches!(e, CounterEvent::Added(1))),
        "the events decode to what was committed",
    );
    assert_eq!(
        events
            .events
            .iter()
            .map(|(seq, _)| seq.value())
            .collect::<Vec<_>>(),
        vec![1, 2, 3],
        "a facetless entity's events sit at consecutive journal slots",
    );
    assert_eq!(
        events.base,
        entity::Seq::ZERO,
        "nothing was compacted, and the page says so",
    );

    // Reading woke nothing: the one activation is the write-time one, and no
    // rehydration followed the hibernation (§10).
    sim.run();
    let activated = recorder
        .events()
        .iter()
        .filter(|e| {
            matches!(
                e.as_app::<EntityEvent>(),
                Some(EntityEvent::Activated { .. })
            )
        })
        .count();
    assert_eq!(activated, 1, "a poll must not wake a hibernated entity");
}

// --- Compaction is visible to readers: the base rides every page (§7.5, §9) ---

#[test]
fn a_compacted_entity_reports_its_base_instead_of_a_silent_gap() {
    let sim = Simulation::new(13);
    let system = LocalSystemBuilder::new(sim.clock(), sim.entropy(), sim.spawner()).build();
    // Snapshot every third event: after four Adds the snapshot at 3 has
    // compacted events 1..=3 away, and only event 4 is still readable.
    let counters = system.entities::<CounterEntity>(EntityConfig {
        snapshot_every: 3,
        ..EntityConfig::default()
    });

    let counter = counters.entity("counter/0");
    sim.block_on(async move {
        for _ in 0..4 {
            counter.ask(Add(1)).await.expect("add commits");
        }
    });

    // A read from ZERO asked for history the snapshot has subsumed: the page
    // reports the base, so the missing prefix is announced truncation, not a
    // silent gap the caller would misread as facet slots (§7.5).
    let reread = counters.entity("counter/0");
    let history = sim.block_on(async move {
        reread
            .events(entity::Seq::ZERO, 100)
            .await
            .expect("a read of a compacted entity succeeds")
    });
    assert_eq!(
        history.base,
        entity::Seq::new(3),
        "the compaction base is the snapshot's seq",
    );
    assert_eq!(
        history
            .events
            .iter()
            .map(|(seq, _)| seq.value())
            .collect::<Vec<_>>(),
        vec![4],
        "only the post-snapshot suffix is still readable",
    );

    // A read from the base asked for nothing that was compacted: complete
    // history from `from`, and the base says so (base <= from).
    let reread = counters.entity("counter/0");
    let from_base = sim.block_on(async move {
        reread
            .events(entity::Seq::new(3), 100)
            .await
            .expect("a read from the base succeeds")
    });
    assert!(
        from_base.base <= entity::Seq::new(3),
        "nothing after `from` was compacted",
    );
    assert_eq!(
        from_base
            .events
            .iter()
            .map(|(seq, _)| seq.value())
            .collect::<Vec<_>>(),
        vec![4],
        "the suffix reads back identically from the base",
    );
}

// --- can_hibernate: an entity that vetoes idle hibernation (§10) ----------------

/// An entity that never permits idle hibernation. The agent runtime overrides
/// `can_hibernate` this way while a run is live (agent §7.2).
#[derive(Default)]
struct PinnedEntity;

impl Entity for PinnedEntity {
    type System = SimSystem;
    type State = CounterState;
    type Event = CounterEvent;
    type Facets = ();
    const ENTITY_TYPE: &'static str = "test.Pinned";

    fn apply(state: &mut CounterState, event: &CounterEvent) {
        match event {
            CounterEvent::Added(d) => state.value += *d,
        }
    }

    fn register(r: &mut entity::EntityRegistry<Self>) {
        r.accept::<Add>();
    }

    fn can_hibernate(&self, _state: &CounterState) -> bool {
        false
    }
}

impl EntityHandler<Add> for PinnedEntity {
    async fn handle(
        &self,
        state: &CounterState,
        msg: Add,
        _ctx: &EntityCtx<Self>,
    ) -> (Vec<CounterEvent>, i64) {
        (vec![CounterEvent::Added(msg.0)], state.value + msg.0)
    }
}

#[test]
fn can_hibernate_vetoes_idle_hibernation() {
    use actor_core::Spawner;

    let sim = Simulation::new(9);
    let recorder = Recorder::new();
    let sink: Arc<dyn EventSink> = Arc::new(recorder.clone());
    let system = LocalSystemBuilder::new(sim.clock(), sim.entropy(), sim.spawner())
        .events(sink)
        .build();
    let pinned = system.entities::<PinnedEntity>(EntityConfig {
        idle_after: Duration::from_millis(10),
        ..EntityConfig::default()
    });

    // Drive with `run_for`, not `block_on`: a vetoing entity reschedules its idle
    // check forever, so the sim never quiesces; a bounded run covers the
    // activation and many idle windows.
    sim.spawner().launch(Box::pin(async move {
        let _ = pinned.entity("p/0").ask(Add(1)).await;
    }));
    sim.run_for(Duration::from_secs(1));

    let events = recorder.events();
    assert!(
        events.iter().any(|e| matches!(
            e.as_app::<EntityEvent>(),
            Some(EntityEvent::Activated { .. })
        )),
        "the entity activated",
    );
    assert!(
        !events.iter().any(|e| matches!(
            e.as_app::<EntityEvent>(),
            Some(EntityEvent::Deactivated { .. })
        )),
        "can_hibernate = false must veto idle hibernation (§10)",
    );
}

// --- An account entity: the Appendix A end-to-end example ----------------------

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

/// An application error: lives inside `M::Reply`, never in `EntityError` (§4.2).
#[derive(Debug, PartialEq, Eq, Serialize, Deserialize)]
struct Overdraft;

impl Entity for Account {
    type System = SimSystem;
    type State = Balance;
    type Event = Ledger;
    type Facets = ();
    const ENTITY_TYPE: &'static str = "bank.Account";

    fn apply(state: &mut Balance, event: &Ledger) {
        match event {
            Ledger::Deposited(n) => state.cents += *n as i64,
            Ledger::Withdrew(n) => state.cents -= *n as i64,
        }
    }

    fn register(r: &mut entity::EntityRegistry<Self>) {
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

impl EntityHandler<Withdraw> for Account {
    async fn handle(
        &self,
        state: &Balance,
        msg: Withdraw,
        _ctx: &EntityCtx<Self>,
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

impl EntityHandler<Deposit> for Account {
    async fn handle(
        &self,
        state: &Balance,
        msg: Deposit,
        _ctx: &EntityCtx<Self>,
    ) -> (Vec<Ledger>, i64) {
        (
            vec![Ledger::Deposited(msg.cents)],
            state.cents + msg.cents as i64,
        )
    }
}

#[derive(Clone, Serialize, Deserialize)]
struct ReadBalance;
impl Message for ReadBalance {
    type Reply = i64;
    const MANIFEST: Manifest = Manifest::new("bank.ReadBalance");
}

impl EntityHandler<ReadBalance> for Account {
    async fn handle(
        &self,
        state: &Balance,
        _msg: ReadBalance,
        _ctx: &EntityCtx<Self>,
    ) -> (Vec<Ledger>, i64) {
        (vec![], state.cents)
    }
}

#[test]
fn account_entity_end_to_end() {
    let sim = Simulation::new(3);
    let system = LocalSystemBuilder::new(sim.clock(), sim.entropy(), sim.spawner()).build();
    let accounts = system.entities::<Account>(EntityConfig::default());

    let acct = accounts.entity("account/42");
    sim.block_on(async move {
        // Committed + durable: the reply reflects post-command state.
        assert_eq!(acct.ask(Deposit { cents: 1000 }).await.unwrap(), 1000);
        assert_eq!(acct.ask(Withdraw { cents: 500 }).await.unwrap(), Ok(500));
        // Application outcome: an error value inside the reply, not an EntityError.
        assert_eq!(
            acct.ask(Withdraw { cents: 9999 }).await.unwrap(),
            Err(Overdraft)
        );
        // The overdrawn withdraw committed nothing (§7.5): balance is unchanged.
        assert_eq!(acct.ask(ReadBalance).await.unwrap(), 500);
    });
}

#[test]
fn tell_commits_fire_and_forget() {
    let sim = Simulation::new(11);
    let system = LocalSystemBuilder::new(sim.clock(), sim.entropy(), sim.spawner()).build();
    let accounts = system.entities::<Account>(EntityConfig::default());

    let acct = accounts.entity("account/99");
    let balance = sim.block_on(async move {
        // `tell` returns once the host accepts the command, not after the commit.
        acct.tell(Deposit { cents: 300 })
            .await
            .expect("tell enqueues");
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
    // host hibernates (§10) the handle goes stale: the next call's `ask`
    // dead-letters, the cache is invalidated, and the command is re-issued through
    // a fresh activation (the command did not run, so the re-issue keeps
    // at-most-once).
    let sim = Simulation::new(21);
    let system = LocalSystemBuilder::new(sim.clock(), sim.entropy(), sim.spawner()).build();
    let accounts = system.entities::<Account>(EntityConfig {
        idle_after: Duration::from_millis(10),
        ..EntityConfig::default()
    });

    let cached_after_first = sim.block_on({
        let accounts = accounts.clone();
        async move {
            let _ = accounts
                .entity("account/1")
                .ask(Deposit { cents: 1000 })
                .await
                .unwrap();
            accounts.is_cached("account/1")
        }
    });
    assert!(
        cached_after_first,
        "the first call must cache the resolved host (§5.4)"
    );

    // Drive past the idle window so the cached host hibernates and stops.
    sim.run();

    // A call through the now-stale cache re-activates and reads the durable balance.
    let balance = sim.block_on({
        let accounts = accounts.clone();
        async move { accounts.entity("account/1").ask(ReadBalance).await.unwrap() }
    });
    assert_eq!(
        balance, 1000,
        "a stale cached host must self-heal, losing no acknowledged write"
    );
}

#[test]
fn account_balance_survives_reactivation() {
    let sim = Simulation::new(5);
    let system = LocalSystemBuilder::new(sim.clock(), sim.entropy(), sim.spawner()).build();
    let accounts = system.entities::<Account>(EntityConfig {
        idle_after: Duration::from_millis(10),
        ..EntityConfig::default()
    });

    let acct = accounts.entity("account/7");
    sim.block_on(async move {
        let _ = acct.ask(Deposit { cents: 250 }).await.unwrap();
    });
    // Hibernate, then read through a fresh activation.
    sim.run();
    let reread = accounts.entity("account/7");
    let balance = sim.block_on(async move { reread.ask(ReadBalance).await.unwrap() });
    assert_eq!(
        balance, 250,
        "a durable deposit must survive hibernation (E12)"
    );
}

#[test]
fn entity_named_hosts_one_entity_under_many_type_names() {
    // The extension point the agent runtime uses (§5.1): one Rust entity (its run
    // loop) hosted under many runtime type names, one per kind. Each name is its
    // own entity type (its own gateway key, shard map, and namespace), so the same
    // key under two names addresses two **independent** entities. The factory also
    // replaces `G::default`, the seam the agent runtime uses to inject per-node
    // handles into each fresh activation.
    let sim = Simulation::new(5);
    let system = LocalSystemBuilder::new(sim.clock(), sim.entropy(), sim.spawner()).build();

    let researchers = system.entity_node().entities_named::<CounterEntity>(
        "agent.researcher",
        EntityConfig::default(),
        Arc::new(CounterEntity::default),
    );
    let summarizers = system.entity_node().entities_named::<CounterEntity>(
        "agent.summarizer",
        EntityConfig::default(),
        Arc::new(CounterEntity::default),
    );

    // The runtime type name lands in the `EntityName`, overriding `G::ENTITY_TYPE`.
    assert_eq!(
        researchers.entity("c/0").name().entity_type(),
        "agent.researcher"
    );
    assert_eq!(
        summarizers.entity("c/0").name().entity_type(),
        "agent.summarizer"
    );

    sim.block_on(async move {
        // Same key under two type names → two independent entities, no crosstalk.
        assert_eq!(researchers.entity("c/0").ask(Add(3)).await.unwrap(), 3);
        assert_eq!(summarizers.entity("c/0").ask(Add(10)).await.unwrap(), 10);
        assert_eq!(researchers.entity("c/0").ask(ReadCount).await.unwrap(), 3);
        assert_eq!(summarizers.entity("c/0").ask(ReadCount).await.unwrap(), 10);
    });
}

// --- E4: a snapshot never shortens the log -----------------------------------
//
// E4 has two halves, upheld in different places.
//
// The load-bearing half is **structural**: a head is *defined* as the best
// snapshot's seq plus the contiguous run of records above it; `ReadReply::head`
// on the `Local` tier, `merge`'s `base` during a `Quorum` recovery. A snapshot can
// therefore never outrun the head it is measured against, on either tier, and that
// is what keeps a snapshot from shortening a log.
//
// The second half is the `s_seq <= head` guard in the host's rehydrate. Given the
// identity above it is **unreachable through any real code path**: defence in
// depth against a future store whose head and snapshot could disagree. The second
// test pins the direction it fails in, with a store double that reports an
// impossible seq: the host must take the journal's authority rather than fold a
// state no committed prefix supports.
//
// The guard can do no more than that. Once a snapshot is taken the records it
// subsumes are compacted away: "a compacted entity's prefix exists only in the
// snapshot" (`replicator.rs`, `migrate_entity`), so replaying from `ZERO` after
// discarding the snapshot yields a *truncated* state, not the true head. Ignoring
// a snapshot is the safe direction (a short state that no longer claims to be
// complete) but it is not a recovery. E4 holds because the head always covers the
// snapshot, never because the guard could rebuild what the snapshot alone held.

/// The structural half of **E4**: a head is the snapshot's seq plus the run above
/// it, so `snapshot.seq <= head` is an identity, not a condition to be checked.
#[test]
fn a_snapshot_never_outruns_the_head_it_is_measured_against() {
    use entity::store::EntityStore;

    let store = entity::store::MemoryEntityStore::new();
    let name = entity::EntityName::new("test.Counter", "g4/structural");
    let shard = 0u32;

    let head_of = |store: &entity::store::MemoryEntityStore| store.head(shard, &name);
    let snapshot_seq_of =
        |store: &entity::store::MemoryEntityStore| store.snapshot(shard, &name).map(|(seq, _)| seq);

    // Interleave appends and snapshots the way a live entity does, checking the
    // identity after every step, including right after a snapshot: the moment a
    // seq could outrun the head if the base were computed any other way.
    for round in 0..6u64 {
        let after = entity::Seq::new(round * 2);
        let _seeded = store.store_record(
            shard,
            &name,
            after,
            entity::store::Term::ZERO,
            vec![vec![round as u8], vec![round as u8]],
            entity::store::WriteKind::Append,
        );

        let head = head_of(&store);
        if let Some(seq) = snapshot_seq_of(&store) {
            assert!(
                seq <= head,
                "round {round}: snapshot at {seq:?} outran head {head:?} after an append",
            );
        }

        // Snapshot at the current head, as the host does (§9).
        let _seeded = store.store_snapshot(
            shard,
            &name,
            head,
            entity::store::Term::ZERO,
            vec![round as u8],
            entity::store::WriteKind::Append,
        );

        let head = head_of(&store);
        let seq = snapshot_seq_of(&store).expect("a snapshot was just stored");
        assert!(
            seq <= head,
            "round {round}: snapshot at {seq:?} outran head {head:?} after a snapshot",
        );
        assert_eq!(
            seq, head,
            "round {round}: a snapshot taken at the head compacts to exactly it",
        );
    }
}

/// The guard's half of **E4**: given a snapshot seq the head does not cover (a
/// state the identity above makes unreachable) the host takes the journal's
/// authority (**E3**) and never folds a state no committed prefix supports.
/// With an untruncated journal that means replaying whole from `ZERO`; here the
/// real snapshot's compaction has already drained the prefix, so no committed
/// prefix exists to replay: the base guard (§9) fails the activation loudly
/// rather than fold the 2 surviving records of 10 and serve the wrong count.
#[test]
fn a_snapshot_beyond_the_committed_head_is_refused_in_favour_of_the_journal() {
    fn run(ahead: u64, recorder: &Recorder, sim: &Simulation) -> Result<i64, entity::EntityError> {
        let sink: Arc<dyn EventSink> = Arc::new(recorder.clone());
        let system = LocalSystemBuilder::new(sim.clock(), sim.entropy(), sim.spawner())
            .events(sink)
            .build();
        let counters = system.entities::<CounterEntity>(EntityConfig {
            idle_after: Duration::from_millis(10),
            snapshot_every: 4,
            // `ahead: 0` is the plain memory store, wrapped identically, so the two
            // runs differ in the reported seq and in nothing else.
            entity_store: Some(entity::testing::SnapshotAheadOfHeadStore::factory(ahead)),
            ..EntityConfig::default()
        });

        let counter = counters.entity("counter/g4");
        sim.block_on(async move {
            for _ in 0..10 {
                counter.ask(Add(1)).await.expect("add commits");
            }
        });
        sim.run(); // snapshot, then hibernate
        assert!(
            recorder.events().iter().any(|e| matches!(
                e.as_app::<EntityEvent>(),
                Some(EntityEvent::Snapshotted { .. })
            )),
            "a snapshot must actually be taken, or the rule is never consulted",
        );
        let reread = counters.entity("counter/g4");
        sim.block_on(async move { reread.ask(ReadCount).await })
    }

    fn rehydrations(recorder: &Recorder) -> Vec<bool> {
        recorder
            .events()
            .into_iter()
            .filter_map(|e| match e.as_app::<EntityEvent>() {
                Some(EntityEvent::Rehydrated { from_snapshot, .. }) => Some(*from_snapshot),
                _ => None,
            })
            .collect()
    }

    // Control: an honest seq. The reactivation seeds from the snapshot (so the
    // refusal below is a real refusal, not an absent snapshot), and reads back
    // the true count.
    let control = Recorder::new();
    let count = run(0, &control, &Simulation::new(23));
    assert_eq!(
        count,
        Ok(10),
        "with an honest snapshot seq, the rehydrated fold is the true count",
    );
    assert_eq!(
        rehydrations(&control),
        vec![false, true],
        "with an honest snapshot seq, reactivation seeds from the snapshot",
    );

    // A seq 100 slots past a ten-record head: refused, and the snapshot's
    // compaction already drained the journal's prefix, so no committed prefix
    // remains to replay. The base guard fails the activation (E3) rather than
    // serve the truncated fold (a count of 2) as truth.
    let overstated = Recorder::new();
    let count = run(100, &overstated, &Simulation::new(23));
    assert!(
        count.is_err(),
        "a lying snapshot over a compacted journal must fail the read, not fold: {count:?}",
    );
    assert!(
        !rehydrations(&overstated).contains(&true),
        "the overstated snapshot is never folded from (E4)",
    );
}
