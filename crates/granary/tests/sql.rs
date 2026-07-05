//! End-to-end SQL facet tests on the `Local` tier (spec §7.14): WAL-frame
//! records committing atomically with the command, zero-record reads, physical
//! replay of nondeterministic SQL, and the checkpoint/rehydration round-trip.
#![cfg(feature = "sql")]

use std::sync::Arc;
use std::time::Duration;

use actor_core::EventSink;
use actor_core::LocalSystemBuilder;
use actor_core::Manifest;
use actor_core::Message;
use actor_simulation::Recorder;
use actor_simulation::SimSystem;
use actor_simulation::Simulation;
use granary::Grain;
use granary::GrainCtx;
use granary::GrainEvent;
use granary::GrainHandler;
use granary::GranaryConfig;
use granary::GranaryExt;
use granary::NoEvent;
use granary::Sql;
use granary::SqlValue;
use serde::Deserialize;
use serde::Serialize;

// --- A grain whose durable state is entirely its SQLite database --------------

#[derive(Default)]
struct Ledger;

impl Grain for Ledger {
    type System = SimSystem;
    type State = ();
    type Event = NoEvent;
    type Facets = (Sql,);
    const GRAIN_TYPE: &'static str = "test.SqlLedger";

    fn apply(_state: &mut (), event: &NoEvent) {
        event.unreachable()
    }
}

/// Idempotent DDL at the top of the writing command (spec §7.14: schema setup
/// is a journaled write like any other; `IF NOT EXISTS` makes it a no-op after
/// the first commit and on every replayed materialization).
fn ensure_schema(ctx: &GrainCtx<Ledger>) {
    ctx.sql()
        .execute(
            "CREATE TABLE IF NOT EXISTS entries (name TEXT NOT NULL, cents INTEGER NOT NULL)",
            &[],
        )
        .expect("ddl");
}

/// Insert one entry; reply with the row count after the insert — read-your-own
/// (transactional) writes inside the command.
#[derive(Clone, Serialize, Deserialize)]
struct Add {
    name: String,
    cents: i64,
}
impl Message for Add {
    type Reply = i64;
    const MANIFEST: Manifest = Manifest::new("test.SqlAdd");
}
impl GrainHandler<Add> for Ledger {
    async fn handle(&self, _state: &(), msg: Add, ctx: &GrainCtx<Self>) -> (Vec<NoEvent>, i64) {
        ensure_schema(ctx);
        let sql = ctx.sql();
        sql.execute(
            "INSERT INTO entries (name, cents) VALUES (?1, ?2)",
            &[SqlValue::Text(msg.name), SqlValue::Integer(msg.cents)],
        )
        .expect("insert");
        let row = sql
            .query_one("SELECT COUNT(*) FROM entries", &[])
            .expect("count");
        let SqlValue::Integer(count) = row[0] else {
            panic!("count is an integer");
        };
        (vec![], count)
    }
}

/// Insert a row whose value SQLite itself draws with `random()`, and reply with
/// it — nondeterministic SQL, fine under physical replication (§7.14, F1 holds
/// on the frames, not the SQL).
#[derive(Clone, Serialize, Deserialize)]
struct AddRandom;
impl Message for AddRandom {
    type Reply = i64;
    const MANIFEST: Manifest = Manifest::new("test.SqlAddRandom");
}
impl GrainHandler<AddRandom> for Ledger {
    async fn handle(
        &self,
        _state: &(),
        _msg: AddRandom,
        ctx: &GrainCtx<Self>,
    ) -> (Vec<NoEvent>, i64) {
        ensure_schema(ctx);
        let sql = ctx.sql();
        sql.execute(
            "INSERT INTO entries (name, cents) VALUES ('random', random() % 1000000)",
            &[],
        )
        .expect("insert random");
        let row = sql
            .query_one(
                "SELECT cents FROM entries WHERE name = 'random' ORDER BY rowid DESC LIMIT 1",
                &[],
            )
            .expect("read back");
        let SqlValue::Integer(value) = row[0] else {
            panic!("cents is an integer");
        };
        (vec![], value)
    }
}

/// The sum of all entries — a pure read: no frames, no record, no commit (§7.5).
#[derive(Clone, Serialize, Deserialize)]
struct Total;
impl Message for Total {
    type Reply = i64;
    const MANIFEST: Manifest = Manifest::new("test.SqlTotal");
}
impl GrainHandler<Total> for Ledger {
    async fn handle(&self, _state: &(), _msg: Total, ctx: &GrainCtx<Self>) -> (Vec<NoEvent>, i64) {
        let row = ctx
            .sql()
            .query_one(
                "SELECT COALESCE(SUM(cents), 0) FROM entries \
                 WHERE name IN (SELECT name FROM entries)",
                &[],
            )
            .expect("sum");
        let SqlValue::Integer(total) = row[0] else {
            panic!("sum is an integer");
        };
        (vec![], total)
    }
}

fn committed_count(recorder: &Recorder) -> usize {
    recorder
        .events()
        .iter()
        .filter(|e| matches!(e.as_app::<GrainEvent>(), Some(GrainEvent::Committed { .. })))
        .count()
}

#[test]
fn sql_writes_survive_hibernation_and_reads_commit_nothing() {
    let dir = tempfile::tempdir().expect("tempdir");
    let sim = Simulation::new(23);
    let recorder = Recorder::new();
    let sink: Arc<dyn EventSink> = Arc::new(recorder.clone());
    let system = LocalSystemBuilder::new(sim.clock(), sim.entropy(), sim.spawner())
        .events(sink)
        .build();
    let ledgers = system.granary::<Ledger>(GranaryConfig {
        idle_after: Duration::from_millis(10),
        snapshot_every: 2, // checkpoint early: rehydration exercises manifest + frames
        data_dir: Some(dir.path().to_path_buf()),
        ..GranaryConfig::default()
    });

    let grain = ledgers.grain("ledger/0");
    let random_value = sim.block_on(async move {
        assert_eq!(
            grain
                .ask(Add {
                    name: "a".into(),
                    cents: 250
                })
                .await
                .expect("add"),
            1
        );
        assert_eq!(
            grain
                .ask(Add {
                    name: "b".into(),
                    cents: 500
                })
                .await
                .expect("add"),
            2
        );
        let random_value = grain.ask(AddRandom).await.expect("add random");
        assert_eq!(
            grain.ask(Total).await.expect("total"),
            750 + random_value,
            "reads see all committed writes",
        );

        // A pure read commits nothing: no frames, no record (§7.5/§7.14).
        let before = grain.ask(Total).await.expect("total");
        let _ = grain.ask(Total).await.expect("total");
        assert_eq!(before, 750 + random_value);
        random_value
    });
    let commits_after_writes = committed_count(&recorder);
    assert_eq!(
        commits_after_writes, 3,
        "exactly the three writing commands committed; reads appended nothing",
    );

    // Hibernate: checkpoint chunks go to blobs, the manifest joins the
    // composite snapshot, and the activation drops its materialization.
    sim.run();
    assert!(
        recorder.events().iter().any(|e| matches!(
            e.as_app::<GrainEvent>(),
            Some(GrainEvent::Passivated { .. })
        )),
        "the idle grain must hibernate",
    );

    // Reactivate: the database rematerializes from the checkpoint manifest plus
    // replayed frame records — including the row SQLite drew with random(),
    // byte-identical (F1 on frames, not on SQL).
    let reread = ledgers.grain("ledger/0");
    sim.block_on(async move {
        assert_eq!(
            reread.ask(Total).await.expect("total after rehydrate"),
            750 + random_value,
            "acknowledged SQL writes survive hibernation (G12), \
             nondeterministic values replay physically",
        );
        assert_eq!(
            reread
                .ask(Add {
                    name: "c".into(),
                    cents: 1
                })
                .await
                .expect("add"),
            4,
            "the rematerialized database accepts new transactions",
        );
    });
}
