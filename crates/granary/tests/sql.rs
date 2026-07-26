//! End-to-end SQL facet tests on the `Local` tier (spec §7.14): WAL-frame
//! records committing atomically with the command, zero-record reads, physical
//! replay of nondeterministic SQL, and the checkpoint/rehydration round-trip.
#![cfg(feature = "sql")]

use std::sync::Arc;
use std::time::Duration;

use actor_core::CallError;
use actor_core::EventSink;
use actor_core::LocalSystemBuilder;
use actor_core::Manifest;
use actor_core::Message;
use actor_simulation::Recorder;
use actor_simulation::SimSystem;
use actor_simulation::Simulation;
use granary::Grain;
use granary::GrainCtx;
use granary::GrainError;
use granary::GrainEvent;
use granary::GrainHandler;
use granary::GranaryConfig;
use granary::GranaryExt;
use granary::MAX_QUERY_ROWS;
use granary::NoEvent;
use granary::Sql;
use granary::SqlValue;
use serde::Deserialize;
use serde::Serialize;

mod support;
use support::Add;
use support::AddRandom;
use support::Ledger;
use support::Total;
use support::ensure_schema;

// --- Suite-specific messages over the shared `Ledger` fixture ----------------

/// Probe the handler-surface guards (spec §7.14): a `select` returns its column
/// names alongside the rows, and the connection authorizer denies `ATTACH` and
/// `PRAGMA` on a handler statement while the facet's own machinery (the schema
/// DDL and the insert above it) runs unrestricted.
#[derive(Clone, Serialize, Deserialize)]
struct Probe;
impl Message for Probe {
    type Reply = (Vec<String>, usize, bool, bool);
    const MANIFEST: Manifest = Manifest::new("test.SqlProbe");
}
impl GrainHandler<Probe> for Ledger {
    async fn handle(
        &self,
        _state: &(),
        _msg: Probe,
        ctx: &GrainCtx<Self>,
    ) -> (Vec<NoEvent>, (Vec<String>, usize, bool, bool)) {
        ensure_schema(ctx);
        let sql = ctx.sql();
        sql.execute("INSERT INTO entries (name, cents) VALUES ('probe', 7)", &[])
            .expect("insert");
        let result = sql
            .select("SELECT name, cents FROM entries", &[])
            .expect("select");
        let attach_denied = sql
            .execute("ATTACH DATABASE 'side.db' AS side", &[])
            .is_err();
        let pragma_denied = sql.execute("PRAGMA journal_mode=DELETE", &[]).is_err();
        (
            vec![],
            (
                result.columns,
                result.rows.len(),
                attach_denied,
                pragma_denied,
            ),
        )
    }
}

/// A command that never touches SQL — the lazy-transaction probe (spec §7.14):
/// it must open neither a connection nor a transaction and commit no record.
#[derive(Clone, Serialize, Deserialize)]
struct Noop;
impl Message for Noop {
    type Reply = ();
    const MANIFEST: Manifest = Manifest::new("test.SqlNoop");
}
impl GrainHandler<Noop> for Ledger {
    async fn handle(&self, _state: &(), _msg: Noop, _ctx: &GrainCtx<Self>) -> (Vec<NoEvent>, ()) {
        (vec![], ())
    }
}

/// Run the full authorizer denial matrix on a handler statement (spec §7.14) —
/// every file-write, pragma, and transaction-control verb — then write again.
/// Replies with the misbehaviors (a verb that ran, or failed for a reason other
/// than authorization); empty means every verb was denied *by the authorizer*
/// and the transaction stayed usable afterwards.
#[derive(Clone, Serialize, Deserialize)]
struct DenyVerbs;
impl Message for DenyVerbs {
    type Reply = Vec<String>;
    const MANIFEST: Manifest = Manifest::new("test.SqlDenyVerbs");
}
impl GrainHandler<DenyVerbs> for Ledger {
    async fn handle(
        &self,
        _state: &(),
        _msg: DenyVerbs,
        ctx: &GrainCtx<Self>,
    ) -> (Vec<NoEvent>, Vec<String>) {
        ensure_schema(ctx);
        let sql = ctx.sql();
        sql.execute("INSERT INTO entries (name, cents) VALUES ('deny', 1)", &[])
            .expect("insert before the denials");
        let denied = [
            "ATTACH DATABASE 'side.db' AS side",
            "DETACH DATABASE side",
            "PRAGMA journal_mode=DELETE",
            "PRAGMA synchronous=FULL",
            "PRAGMA wal_checkpoint(TRUNCATE)",
            "BEGIN",
            "COMMIT",
            "ROLLBACK",
            "SAVEPOINT sp",
            "RELEASE sp",
        ];
        let mut misbehaviors = Vec::new();
        for stmt in denied {
            match sql.execute(stmt, &[]) {
                Ok(_) => misbehaviors.push(format!("{stmt}: ran")),
                Err(e) if !e.to_string().contains("authorized") => {
                    misbehaviors.push(format!("{stmt}: failed for the wrong reason: {e}"));
                }
                Err(_) => {}
            }
        }
        // The denials must not poison the command's transaction: a later write
        // runs and commits with the batch (the `Restricted` guard restored the
        // authorizer for the facet's own COMMIT at seal).
        sql.execute("INSERT INTO entries (name, cents) VALUES ('deny', 2)", &[])
            .expect("insert after the denials");
        (vec![], misbehaviors)
    }
}

/// Count the rows a prior command committed under a name — the read-back probe.
#[derive(Clone, Serialize, Deserialize)]
struct CountNamed(String);
impl Message for CountNamed {
    type Reply = i64;
    const MANIFEST: Manifest = Manifest::new("test.SqlCountNamed");
}
impl GrainHandler<CountNamed> for Ledger {
    async fn handle(
        &self,
        _state: &(),
        msg: CountNamed,
        ctx: &GrainCtx<Self>,
    ) -> (Vec<NoEvent>, i64) {
        ensure_schema(ctx);
        let row = ctx
            .sql()
            .query_one(
                "SELECT COUNT(*) FROM entries WHERE name = ?1",
                &[SqlValue::Text(msg.0)],
            )
            .expect("count");
        let SqlValue::Integer(count) = row[0] else {
            panic!("count is an integer");
        };
        (vec![], count)
    }
}

/// Probe the [`MAX_QUERY_ROWS`] cap (spec §7.14): bulk-insert one row past the
/// cap, then reply with (the over-cap `select` error, the row count `select`
/// returns at exactly the cap, the over-cap `query` error — `query` enforces
/// the same cap).
#[derive(Clone, Serialize, Deserialize)]
struct BulkSelect;
impl Message for BulkSelect {
    type Reply = (String, usize, String);
    const MANIFEST: Manifest = Manifest::new("test.SqlBulkSelect");
}
impl GrainHandler<BulkSelect> for Ledger {
    async fn handle(
        &self,
        _state: &(),
        _msg: BulkSelect,
        ctx: &GrainCtx<Self>,
    ) -> (Vec<NoEvent>, (String, usize, String)) {
        ensure_schema(ctx);
        let sql = ctx.sql();
        sql.execute(
            "WITH RECURSIVE cnt(x) AS (SELECT 1 UNION ALL SELECT x + 1 FROM cnt WHERE x < ?1) \
             INSERT INTO entries (name, cents) SELECT 'bulk', x FROM cnt",
            &[SqlValue::Integer(MAX_QUERY_ROWS as i64 + 1)],
        )
        .expect("bulk insert");
        let over = sql
            .select("SELECT cents FROM entries WHERE name = 'bulk'", &[])
            .expect_err("one row past the cap must be an error, not a truncation")
            .to_string();
        let at_cap = sql
            .select(
                "SELECT cents FROM entries WHERE name = 'bulk' LIMIT ?1",
                &[SqlValue::Integer(MAX_QUERY_ROWS as i64)],
            )
            .expect("exactly the cap is fine")
            .rows
            .len();
        let over_query = sql
            .query("SELECT cents FROM entries WHERE name = 'bulk'", &[])
            .expect_err("query enforces the same cap as select")
            .to_string();
        (vec![], (over, at_cap, over_query))
    }
}

/// Probe [`SqlHandle::query_one`]'s cardinality contract in-handler: reply with
/// the error strings for a zero-row and a two-row query.
#[derive(Clone, Serialize, Deserialize)]
struct QueryOneProbe;
impl Message for QueryOneProbe {
    type Reply = (String, String);
    const MANIFEST: Manifest = Manifest::new("test.SqlQueryOneProbe");
}
impl GrainHandler<QueryOneProbe> for Ledger {
    async fn handle(
        &self,
        _state: &(),
        _msg: QueryOneProbe,
        ctx: &GrainCtx<Self>,
    ) -> (Vec<NoEvent>, (String, String)) {
        ensure_schema(ctx);
        let sql = ctx.sql();
        sql.execute(
            "INSERT INTO entries (name, cents) VALUES ('two', 1), ('two', 2)",
            &[],
        )
        .expect("insert");
        let zero = sql
            .query_one("SELECT cents FROM entries WHERE name = 'none'", &[])
            .expect_err("zero rows must error")
            .to_string();
        let two = sql
            .query_one("SELECT cents FROM entries WHERE name = 'two'", &[])
            .expect_err("two rows must error")
            .to_string();
        (vec![], (zero, two))
    }
}

// --- A grain whose event encoding fails: the abandoned-command path ------------

/// An event that refuses to encode, forcing the host's step-2 serialization
/// failure: the command is abandoned *after* the handler ran its SQL, leaving
/// the facet's transaction open for the next command's clean-slate rollback
/// (spec §7.14 — an abandoned command never reaches `seal`).
#[derive(Clone, Debug)]
struct Unencodable;

impl Serialize for Unencodable {
    fn serialize<S: serde::Serializer>(&self, _s: S) -> Result<S::Ok, S::Error> {
        Err(serde::ser::Error::custom("unencodable by design"))
    }
}

impl<'de> Deserialize<'de> for Unencodable {
    fn deserialize<D: serde::Deserializer<'de>>(_d: D) -> Result<Unencodable, D::Error> {
        Err(serde::de::Error::custom("never encoded, so never decoded"))
    }
}

#[derive(Default)]
struct Flaky;

impl Grain for Flaky {
    type System = SimSystem;
    type State = ();
    type Event = Unencodable;
    type Facets = (Sql,);
    const GRAIN_TYPE: &'static str = "test.SqlFlaky";

    fn apply(_state: &mut (), _event: &Unencodable) {
        unreachable!("an Unencodable event never commits");
    }
}

/// Insert one row; when `fail` is set, also emit the event whose encoding
/// fails, so the host abandons the command after the insert ran.
#[derive(Clone, Serialize, Deserialize)]
struct FlakyInsert {
    fail: bool,
}
impl Message for FlakyInsert {
    type Reply = ();
    const MANIFEST: Manifest = Manifest::new("test.SqlFlakyInsert");
}
impl GrainHandler<FlakyInsert> for Flaky {
    async fn handle(
        &self,
        _state: &(),
        msg: FlakyInsert,
        ctx: &GrainCtx<Self>,
    ) -> (Vec<Unencodable>, ()) {
        let sql = ctx.sql();
        sql.execute("CREATE TABLE IF NOT EXISTS t (x INTEGER)", &[])
            .expect("ddl");
        sql.execute("INSERT INTO t (x) VALUES (1)", &[])
            .expect("insert");
        let events = if msg.fail { vec![Unencodable] } else { vec![] };
        (events, ())
    }
}

/// Count the committed rows — the visibility probe for the rolled-back insert.
#[derive(Clone, Serialize, Deserialize)]
struct FlakyCount;
impl Message for FlakyCount {
    type Reply = i64;
    const MANIFEST: Manifest = Manifest::new("test.SqlFlakyCount");
}
impl GrainHandler<FlakyCount> for Flaky {
    async fn handle(
        &self,
        _state: &(),
        _msg: FlakyCount,
        ctx: &GrainCtx<Self>,
    ) -> (Vec<Unencodable>, i64) {
        let sql = ctx.sql();
        sql.execute("CREATE TABLE IF NOT EXISTS t (x INTEGER)", &[])
            .expect("ddl");
        let row = sql.query_one("SELECT COUNT(*) FROM t", &[]).expect("count");
        let SqlValue::Integer(count) = row[0] else {
            panic!("count is an integer");
        };
        (vec![], count)
    }
}

fn committed_count(recorder: &Recorder) -> usize {
    recorder
        .events()
        .iter()
        .filter(|e| matches!(e.as_app::<GrainEvent>(), Some(GrainEvent::Committed { .. })))
        .count()
}

/// Count the `*.db` materialization files under a scratch tree — the on-disk
/// witness of whether the facet ever opened a connection.
fn db_file_count(dir: &std::path::Path) -> usize {
    let mut count = 0;
    let mut stack = vec![dir.to_path_buf()];
    while let Some(d) = stack.pop() {
        let Ok(entries) = std::fs::read_dir(&d) else {
            continue;
        };
        for entry in entries.flatten() {
            let path = entry.path();
            if path.is_dir() {
                stack.push(path);
            } else if path.extension().is_some_and(|e| e == "db") {
                count += 1;
            }
        }
    }
    count
}

#[test]
fn a_command_that_runs_no_sql_opens_no_connection_and_commits_nothing() {
    // The lazy-transaction contract (spec §7.14): a grain that carries the facet
    // but whose command never touches SQL pays nothing — no connection (so no
    // database file materializes), no BEGIN, no record, no `Committed`.
    let dir = tempfile::tempdir().expect("tempdir");
    let sim = Simulation::new(7);
    let recorder = Recorder::new();
    let sink: Arc<dyn EventSink> = Arc::new(recorder.clone());
    let system = LocalSystemBuilder::new(sim.clock(), sim.entropy(), sim.spawner())
        .events(sink)
        .build();
    let ledgers = system.granary::<Ledger>(GranaryConfig {
        idle_after: Duration::from_secs(60),
        data_dir: Some(dir.path().to_path_buf()),
        ..GranaryConfig::default()
    });
    let grain = ledgers.grain("lazy/0");
    sim.block_on(async move { grain.ask(Noop).await.expect("noop") });
    assert_eq!(
        committed_count(&recorder),
        0,
        "a no-SQL command commits no record"
    );
    assert_eq!(
        db_file_count(dir.path()),
        0,
        "a no-SQL command must not even open the connection (§7.14)"
    );

    // The Active → Open transition still works after a no-SQL command: the next
    // command's first statement opens the transaction and commits normally.
    let grain = ledgers.grain("lazy/0");
    sim.block_on(async move {
        assert_eq!(
            grain
                .ask(Add {
                    name: "a".into(),
                    cents: 1
                })
                .await
                .expect("add"),
            1
        );
    });
    assert_eq!(committed_count(&recorder), 1);
    assert_eq!(
        db_file_count(dir.path()),
        1,
        "the first SQL statement is what materializes the database"
    );
}

#[test]
fn the_authorizer_denies_every_escape_verb_and_the_transaction_survives() {
    // The full denial matrix (spec §7.14): ATTACH/DETACH (file escape), PRAGMA
    // (facet-fixed modes), and BEGIN/COMMIT/ROLLBACK/SAVEPOINT/RELEASE (the
    // facet owns the transaction boundary). Each must fail *as an authorization
    // denial*, and the command's own transaction must stay usable and commit.
    let dir = tempfile::tempdir().expect("tempdir");
    let sim = Simulation::new(11);
    let system = LocalSystemBuilder::new(sim.clock(), sim.entropy(), sim.spawner()).build();
    let ledgers = system.granary::<Ledger>(GranaryConfig {
        idle_after: Duration::from_secs(60),
        data_dir: Some(dir.path().to_path_buf()),
        ..GranaryConfig::default()
    });
    let grain = ledgers.grain("deny/0");
    let reader = ledgers.grain("deny/0");
    sim.block_on(async move {
        let misbehaviors = grain.ask(DenyVerbs).await.expect("deny probe");
        assert!(
            misbehaviors.is_empty(),
            "every escape verb must be denied by the authorizer:\n{}",
            misbehaviors.join("\n")
        );
        // Both writes — before and after the denials — committed with the batch:
        // the denials neither rolled back nor poisoned the transaction, and the
        // `Restricted` guard restored the authorizer for the facet's own COMMIT.
        assert_eq!(
            reader
                .ask(CountNamed("deny".into()))
                .await
                .expect("read back"),
            2,
            "writes around the denied statements commit (§7.14)"
        );
    });
}

#[test]
fn select_errors_past_the_row_cap_instead_of_truncating() {
    let dir = tempfile::tempdir().expect("tempdir");
    let sim = Simulation::new(13);
    let system = LocalSystemBuilder::new(sim.clock(), sim.entropy(), sim.spawner()).build();
    let ledgers = system.granary::<Ledger>(GranaryConfig {
        idle_after: Duration::from_secs(60),
        data_dir: Some(dir.path().to_path_buf()),
        ..GranaryConfig::default()
    });
    let grain = ledgers.grain("bulk/0");
    let (over, at_cap, over_query) =
        sim.block_on(async move { grain.ask(BulkSelect).await.expect("bulk probe") });
    assert!(
        over.contains("more than"),
        "one row past the cap is an error, never a silent truncation (§7.14): {over}"
    );
    assert_eq!(at_cap, MAX_QUERY_ROWS, "exactly the cap succeeds");
    assert!(
        over_query.contains("more than"),
        "`query` enforces the same cap as `select` (§7.14): {over_query}"
    );
}

#[test]
fn an_abandoned_command_rolls_back_its_transaction_before_the_next_one() {
    // The clean-slate guard (spec §7.14): a command abandoned after its SQL ran
    // (here: its event fails to encode, so the host abandons the stage without
    // sealing) leaves an open transaction; the next command's `begin` must roll
    // it back — the un-journaled insert is invisible — and the facet must be
    // fully usable afterwards.
    let dir = tempfile::tempdir().expect("tempdir");
    let sim = Simulation::new(17);
    let system = LocalSystemBuilder::new(sim.clock(), sim.entropy(), sim.spawner()).build();
    let flaky = system.granary::<Flaky>(GranaryConfig {
        idle_after: Duration::from_secs(60),
        data_dir: Some(dir.path().to_path_buf()),
        ..GranaryConfig::default()
    });
    let grain = flaky.grain("flaky/0");
    sim.block_on(async move {
        grain
            .ask(FlakyInsert { fail: false })
            .await
            .expect("a clean insert commits");
        assert_eq!(grain.ask(FlakyCount).await.expect("count"), 1);

        let err = grain
            .ask(FlakyInsert { fail: true })
            .await
            .expect_err("an unencodable event must fail the command");
        assert!(
            matches!(err, GrainError::Call(CallError::Serialization(_))),
            "the failure is the step-2 serialization abandon, got: {err:?}"
        );

        assert_eq!(
            grain.ask(FlakyCount).await.expect("count after abandon"),
            1,
            "the abandoned command's insert was rolled back, never journaled (§7.14/G20)"
        );
        grain
            .ask(FlakyInsert { fail: false })
            .await
            .expect("the facet is reusable after the rollback");
        assert_eq!(grain.ask(FlakyCount).await.expect("final count"), 2);
    });
}

#[test]
fn query_one_rejects_zero_and_many_rows() {
    let dir = tempfile::tempdir().expect("tempdir");
    let sim = Simulation::new(19);
    let system = LocalSystemBuilder::new(sim.clock(), sim.entropy(), sim.spawner()).build();
    let ledgers = system.granary::<Ledger>(GranaryConfig {
        idle_after: Duration::from_secs(60),
        data_dir: Some(dir.path().to_path_buf()),
        ..GranaryConfig::default()
    });
    let grain = ledgers.grain("one/0");
    let (zero, two) = sim.block_on(async move { grain.ask(QueryOneProbe).await.expect("probe") });
    assert!(
        zero.contains("expected one row, got 0"),
        "query_one over zero rows errors: {zero}"
    );
    assert!(
        two.contains("expected one row, got 2"),
        "query_one over two rows errors: {two}"
    );
}

#[test]
fn select_names_columns_and_the_authorizer_denies_dangerous_statements() {
    let dir = tempfile::tempdir().expect("tempdir");
    let sim = Simulation::new(41);
    let system = LocalSystemBuilder::new(sim.clock(), sim.entropy(), sim.spawner()).build();
    let ledgers = system.granary::<Ledger>(GranaryConfig {
        idle_after: Duration::from_millis(10),
        data_dir: Some(dir.path().to_path_buf()),
        ..GranaryConfig::default()
    });
    let grain = ledgers.grain("probe/0");
    let (columns, rows, attach_denied, pragma_denied) =
        sim.block_on(async move { grain.ask(Probe).await.expect("probe") });
    assert_eq!(
        columns,
        vec!["name".to_string(), "cents".to_string()],
        "select carries column names"
    );
    assert!(rows >= 1, "select returns the inserted row");
    assert!(
        attach_denied,
        "the authorizer denies ATTACH on a handler statement (§7.14)"
    );
    assert!(
        pragma_denied,
        "the authorizer denies PRAGMA on a handler statement (§7.14)"
    );
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

// --- The seeded swarm (V&V checklist #4, #7) -----------------------------------
