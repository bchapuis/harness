//! The `Ledger` grain: the shared SQL-facet fixture (spec §7.14).
//!
//! A grain whose durable state is entirely its SQLite database, plus the three
//! messages both SQL suites drive it with. Shared by `sql.rs` (scenarios) and
//! `sql_swarm.rs` (sweeps) so the two exercise the *same* grain — a fixture that
//! drifted between them would make the sweep stop covering what the scenarios
//! specify.
//!
//! Suite-specific messages stay with their suite; only what both need lives here.

use actor_core::Manifest;
use actor_core::Message;
use actor_simulation::SimSystem;
use granary::Grain;
use granary::GrainCtx;
use granary::GrainHandler;
use granary::NoEvent;
use granary::Sql;
use granary::SqlValue;
use serde::Deserialize;
use serde::Serialize;

#[derive(Default)]
pub struct Ledger;

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
pub fn ensure_schema(ctx: &GrainCtx<Ledger>) {
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
pub struct Add {
    pub name: String,
    pub cents: i64,
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
pub struct AddRandom;
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
pub struct Total;
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
