//! The SQL facet (spec §7.14): a private, on-disk SQLite database per grain.
//!
//! The first **physical facet** (§7.12): the handler runs ordinary SQL through
//! [`GrainCtx::sql`](crate::GrainCtx::sql) against a local database file —
//! zero-latency reads and writes, the Durable Objects storage model itself
//! (DO §2.3, §4.2) — and durability comes from shipping each committed
//! transaction's **WAL frames** as a tagged record in the command's atomic
//! batch. Replication is physical, never re-execution, so nondeterministic SQL
//! (`random()`, `datetime('now')`, autoincrement) is fine (F1 holds
//! byte-for-byte on the frames).
//!
//! **One command, one transaction, one record.** [`Facet::begin`] opens
//! `BEGIN IMMEDIATE`; the handler's statements run inside it; [`Facet::seal`]
//! commits locally and captures the transaction's frames by **WAL tailing**
//! (autocheckpoint is off; the host owns checkpoint timing — the
//! Litestream-proven mechanism; a capture VFS is the deferred upgrade, §16). A
//! read-only transaction appends nothing to the WAL, so it produces no record
//! and rides the §7.5 read path.
//!
//! **The physical discipline (G20/F4).** The database file mutates at local
//! commit, before durability. On `Committed` the host keeps the
//! materialization (the live fold is skipped — the delta is already applied);
//! on any other outcome it [`Facet::discard`]s the files outright, and the next
//! activation rematerializes from the composite snapshot plus committed frame
//! records. The local file is a cache (§1), which is why
//! `PRAGMA synchronous=OFF` is sound: durability is the quorum ack, never the
//! local disk.
//!
//! **Checkpoints are the snapshot contribution.** At snapshot time the WAL is
//! checkpointed (`TRUNCATE`) into the database file, the file is chunked into
//! **page-aligned content-addressed blocks** stored as blobs (§7.10), and the
//! contribution is a small manifest of their ids. Unchanged chunks hash to
//! blobs already stored, so a checkpoint uploads only dirty regions —
//! incremental by dedup. The manifests' chunk ids are the facet's blob roots
//! (F3); §9 compaction then drops the frame records the checkpoint subsumes.

use std::fs;
use std::io::Read;
use std::io::Seek;
use std::io::SeekFrom;
use std::io::Write;
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::Mutex;

use rusqlite::Connection;
use serde::Deserialize;
use serde::Serialize;

use crate::blobs::BlobId;
use crate::facet::Facet;
use crate::facet::FacetCell;
use crate::facet::FacetEnv;
use crate::facet::FacetError;
use crate::facet::HasFacet;
use crate::facet::sealed::Sealed;
use crate::grain::Grain;
use crate::grain::GrainCtx;

/// The standard SQLite page size the facet fixes for every grain database.
const PAGE_SIZE: u32 = 4096;
/// The SQLite WAL file header length.
const WAL_HEADER: u64 = 32;
/// The per-frame header length (page number, db size, salts, checksums).
const FRAME_HEADER: u64 = 24;
/// Checkpoint chunk size: 64 pages (256 KiB) per content-addressed block —
/// page-aligned so an unchanged region hashes to a blob already stored.
const CHUNK_BYTES: usize = (PAGE_SIZE as usize) * 64;

/// The SQL facet marker (spec §7.14): declare `type Facets = (Sql, …)` and
/// reach the database through [`GrainCtx::sql`](crate::GrainCtx::sql).
pub struct Sql;

impl Sealed for Sql {}

/// A SQL statement failed — an *application-level* outcome the handler maps
/// into its reply, distinct from a durability failure (§12).
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct SqlError(pub String);

impl std::fmt::Display for SqlError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "sql error: {}", self.0)
    }
}

impl std::error::Error for SqlError {}

/// One SQL value, in and out of the database (spec §7.14). Serializable so a
/// grain can carry query results in its replies.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub enum SqlValue {
    Null,
    Integer(i64),
    Real(f64),
    Text(String),
    Blob(Vec<u8>),
}

impl rusqlite::ToSql for SqlValue {
    fn to_sql(&self) -> rusqlite::Result<rusqlite::types::ToSqlOutput<'_>> {
        use rusqlite::types::ToSqlOutput;
        use rusqlite::types::ValueRef;
        Ok(match self {
            SqlValue::Null => ToSqlOutput::Borrowed(ValueRef::Null),
            SqlValue::Integer(i) => ToSqlOutput::Borrowed(ValueRef::Integer(*i)),
            SqlValue::Real(r) => ToSqlOutput::Borrowed(ValueRef::Real(*r)),
            SqlValue::Text(s) => ToSqlOutput::Borrowed(ValueRef::Text(s.as_bytes())),
            SqlValue::Blob(b) => ToSqlOutput::Borrowed(ValueRef::Blob(b)),
        })
    }
}

impl From<rusqlite::types::ValueRef<'_>> for SqlValue {
    fn from(value: rusqlite::types::ValueRef<'_>) -> SqlValue {
        use rusqlite::types::ValueRef;
        match value {
            ValueRef::Null => SqlValue::Null,
            ValueRef::Integer(i) => SqlValue::Integer(i),
            ValueRef::Real(r) => SqlValue::Real(r),
            ValueRef::Text(t) => SqlValue::Text(String::from_utf8_lossy(t).into_owned()),
            ValueRef::Blob(b) => SqlValue::Blob(b.to_vec()),
        }
    }
}

/// One row of a query result.
pub type SqlRow = Vec<SqlValue>;

/// The captured physical delta of one committed transaction (spec §7.14): the
/// WAL frames — page number and page bytes — plus the database size after the
/// commit. The facet's record payload, applied byte-deterministically on
/// replay (F1).
#[derive(Serialize, Deserialize)]
struct SqlDelta {
    page_size: u32,
    /// Database size in pages after this transaction's commit frame.
    db_pages: u32,
    frames: Vec<(u32, Vec<u8>)>,
}

/// The checkpoint manifest (spec §7.14): the facet's composite-snapshot
/// contribution. The database image at the snapshot seq, as page-aligned
/// content-addressed chunks in the grain's blob area.
#[derive(Serialize, Deserialize)]
struct SqlManifest {
    page_size: u32,
    db_bytes: u64,
    chunk_bytes: u32,
    chunks: Vec<BlobId>,
}

/// The materialization handle: the database file path, the lazily-opened
/// connection, the WAL capture cursor, and the live checkpoint-chunk roots.
/// Shared by `Arc` so a forms clone (the host's snapshot path) sees the same
/// materialization.
struct SqlDb {
    path: PathBuf,
    conn: Mutex<Option<Connection>>,
    /// Whether a facet-opened transaction is active — SQL access through the
    /// handle is valid only inside one (spec §7.14: one command, one
    /// transaction; a write outside a command would produce frames no record
    /// captures).
    in_txn: Mutex<bool>,
    /// Bytes of the WAL already captured into records.
    tail: Mutex<u64>,
    /// The checkpoint-chunk ids this activation must keep alive (F3): the
    /// restored manifest's plus every later checkpoint's. The union is kept —
    /// never pruned mid-activation — so a failed `save_snapshot` can never
    /// leave the *current* durable manifest's chunks sweepable; the next
    /// activation restores from the durable manifest and resets the set.
    roots: Mutex<Vec<BlobId>>,
}

impl SqlDb {
    fn wal_path(&self) -> PathBuf {
        let mut os = self.path.clone().into_os_string();
        os.push("-wal");
        PathBuf::from(os)
    }

    fn shm_path(&self) -> PathBuf {
        let mut os = self.path.clone().into_os_string();
        os.push("-shm");
        PathBuf::from(os)
    }

    /// Open the connection if not yet open, fixing the facet's pragmas: WAL
    /// journaling (the capture substrate), `synchronous=OFF` (the local file is
    /// a cache; durability is the quorum, §7.14), autocheckpoint off (the host
    /// owns checkpoint timing).
    fn ensure_conn(&self) -> Result<(), FacetError> {
        let mut conn = self.conn.lock().expect("sql conn lock");
        if conn.is_some() {
            return Ok(());
        }
        let opened = Connection::open(&self.path).map_err(sql_facet_err)?;
        opened
            .pragma_update(None, "page_size", PAGE_SIZE)
            .map_err(sql_facet_err)?;
        let mode: String = opened
            .query_row("PRAGMA journal_mode=WAL", [], |row| row.get(0))
            .map_err(sql_facet_err)?;
        if mode != "wal" {
            return Err(FacetError(format!("sql: journal_mode is {mode}, not wal")));
        }
        opened
            .pragma_update(None, "synchronous", "OFF")
            .map_err(sql_facet_err)?;
        let _autockpt: i64 = opened
            .query_row("PRAGMA wal_autocheckpoint=0", [], |row| row.get(0))
            .map_err(sql_facet_err)?;
        *conn = Some(opened);
        Ok(())
    }

    /// Parse the WAL frames appended since the capture cursor — everything the
    /// just-committed transaction wrote — and advance the cursor.
    fn capture(&self) -> Result<Option<SqlDelta>, FacetError> {
        let wal = self.wal_path();
        let len = fs::metadata(&wal).map(|m| m.len()).unwrap_or(0);
        let mut tail = self.tail.lock().expect("sql tail lock");
        if len < *tail {
            // The WAL restarted (a checkpoint truncated it); frames begin anew.
            *tail = 0;
        }
        let start = (*tail).max(WAL_HEADER);
        if len <= start {
            return Ok(None);
        }
        let mut file = fs::File::open(&wal).map_err(io_facet_err)?;
        let mut header = [0u8; WAL_HEADER as usize];
        file.read_exact(&mut header).map_err(io_facet_err)?;
        let raw_ps = u32::from_be_bytes([header[8], header[9], header[10], header[11]]);
        let page_size = if raw_ps == 1 { 65536 } else { raw_ps };
        if page_size != PAGE_SIZE {
            return Err(FacetError(format!(
                "sql: wal page size {page_size}, facet fixes {PAGE_SIZE}"
            )));
        }
        let frame_len = FRAME_HEADER + page_size as u64;
        // Read every whole appended frame in one pass (a torn trailing partial
        // frame waits for the next capture) rather than two syscalls per frame.
        let count = (len - start) / frame_len;
        if count == 0 {
            return Ok(None);
        }
        let mut region = vec![0u8; (count * frame_len) as usize];
        file.seek(SeekFrom::Start(start)).map_err(io_facet_err)?;
        file.read_exact(&mut region).map_err(io_facet_err)?;
        let mut frames = Vec::with_capacity(count as usize);
        let mut db_pages = 0u32;
        for frame in region.chunks_exact(frame_len as usize) {
            let pgno = u32::from_be_bytes([frame[0], frame[1], frame[2], frame[3]]);
            let after = u32::from_be_bytes([frame[4], frame[5], frame[6], frame[7]]);
            if after != 0 {
                db_pages = after; // a commit frame carries the db size
            }
            frames.push((pgno, frame[FRAME_HEADER as usize..].to_vec()));
        }
        *tail = start + count * frame_len;
        if db_pages == 0 {
            // Frames with no commit frame after a COMMIT: a torn tail we must
            // not ship (the record would not reproduce a committed state).
            return Err(FacetError("sql: captured frames carry no commit".into()));
        }
        Ok(Some(SqlDelta {
            page_size,
            db_pages,
            frames,
        }))
    }

    /// Apply one captured delta to the database file — the replay fold (F1):
    /// write each frame's page at its offset, in order, then fix the file
    /// length to the committed page count. Byte-deterministic.
    fn apply_delta(&self, delta: &SqlDelta) -> Result<(), FacetError> {
        // Replay never runs with a live connection (it is lazily opened on the
        // first command); drop one defensively so no stale pager cache
        // survives the direct file writes.
        drop(self.conn.lock().expect("sql conn lock").take());
        let mut file = fs::OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .open(&self.path)
            .map_err(io_facet_err)?;
        for (pgno, page) in &delta.frames {
            if *pgno == 0 || page.len() != delta.page_size as usize {
                return Err(FacetError("sql: malformed frame record".into()));
            }
            file.seek(SeekFrom::Start((*pgno as u64 - 1) * delta.page_size as u64))
                .map_err(io_facet_err)?;
            file.write_all(page).map_err(io_facet_err)?;
        }
        file.set_len(delta.db_pages as u64 * delta.page_size as u64)
            .map_err(io_facet_err)?;
        Ok(())
    }

    /// Fold the WAL into the database file (`TRUNCATE` checkpoint) so the file
    /// alone is the committed image, and reset the capture cursor.
    fn checkpoint(&self) -> Result<(), FacetError> {
        let conn = self.conn.lock().expect("sql conn lock");
        if let Some(conn) = conn.as_ref() {
            let (busy, _logged, _moved): (i64, i64, i64) = conn
                .query_row("PRAGMA wal_checkpoint(TRUNCATE)", [], |row| {
                    Ok((row.get(0)?, row.get(1)?, row.get(2)?))
                })
                .map_err(sql_facet_err)?;
            if busy != 0 {
                return Err(FacetError("sql: checkpoint was blocked".into()));
            }
        }
        *self.tail.lock().expect("sql tail lock") = 0;
        Ok(())
    }

    /// Delete the materialization — the discard of G20. The next activation
    /// rebuilds from the composite snapshot plus committed frame records.
    fn delete_files(&self) {
        drop(self.conn.lock().expect("sql conn lock").take());
        let _ = fs::remove_file(self.wal_path());
        let _ = fs::remove_file(self.shm_path());
        let _ = fs::remove_file(&self.path);
        *self.tail.lock().expect("sql tail lock") = 0;
    }
}

/// The committed form: `None` until [`Facet::restore`] materializes (the host
/// always restores on rehydration, snapshot or not).
#[derive(Clone, Default)]
pub struct SqlForm(Option<Arc<SqlDb>>);

impl SqlForm {
    fn db(&self) -> Result<&Arc<SqlDb>, FacetError> {
        self.0
            .as_ref()
            .ok_or_else(|| FacetError("sql: database not materialized".into()))
    }
}

/// The per-command stage: the delta captured at seal, if the transaction wrote.
#[derive(Default)]
pub struct SqlStage {
    delta: Option<Vec<u8>>,
}

impl Facet for Sql {
    const TAG: u8 = 3;
    const PHYSICAL: bool = true;

    type Form = SqlForm;
    type Stage = SqlStage;

    fn begin(form: &mut SqlForm, _stage: &mut SqlStage) -> Result<(), FacetError> {
        let db = form.db()?;
        db.ensure_conn()?;
        let conn = db.conn.lock().expect("sql conn lock");
        conn.as_ref()
            .expect("ensure_conn opened")
            .execute_batch("BEGIN IMMEDIATE")
            .map_err(sql_facet_err)?;
        *db.in_txn.lock().expect("sql txn lock") = true;
        Ok(())
    }

    fn seal(form: &mut SqlForm, stage: &mut SqlStage) -> Result<(), FacetError> {
        let db = form.db()?;
        {
            let conn = db.conn.lock().expect("sql conn lock");
            let conn = conn
                .as_ref()
                .ok_or_else(|| FacetError("sql: connection closed mid-command".into()))?;
            conn.execute_batch("COMMIT").map_err(sql_facet_err)?;
        }
        *db.in_txn.lock().expect("sql txn lock") = false;
        if let Some(delta) = db.capture()? {
            stage.delta = Some(crate::facet::encode_payload(&delta));
        }
        Ok(())
    }

    fn drain(stage: SqlStage) -> Vec<Vec<u8>> {
        stage.delta.into_iter().collect()
    }

    fn fold(form: &mut SqlForm, payload: &[u8]) -> Result<(), FacetError> {
        // Replay only: the live path skips physical facets (§7.12) — the delta
        // was applied by the local commit that captured it.
        let delta: SqlDelta = crate::facet::decode_payload("sql record", payload)?;
        form.db()?.apply_delta(&delta)
    }

    fn discard(form: &mut SqlForm) {
        if let Some(db) = &form.0 {
            db.delete_files();
        }
        form.0 = None;
    }

    async fn snapshot(form: &SqlForm, env: &FacetEnv) -> Result<Vec<u8>, FacetError> {
        let db = form.db()?;
        db.checkpoint()?;
        // Read the checkpointed image in page-aligned chunks. No file yet means
        // an empty database (no write ever ran).
        let mut parts: Vec<Vec<u8>> = Vec::new();
        let mut db_bytes = 0u64;
        match fs::File::open(&db.path) {
            Ok(mut file) => loop {
                let mut chunk = vec![0u8; CHUNK_BYTES];
                let mut filled = 0;
                while filled < CHUNK_BYTES {
                    let n = file.read(&mut chunk[filled..]).map_err(io_facet_err)?;
                    if n == 0 {
                        break;
                    }
                    filled += n;
                }
                if filled == 0 {
                    break;
                }
                chunk.truncate(filled);
                db_bytes += filled as u64;
                parts.push(chunk);
                if filled < CHUNK_BYTES {
                    break;
                }
            },
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
            Err(e) => return Err(io_facet_err(e)),
        }
        // The chunk puts are independent; issue them concurrently.
        let puts = parts.into_iter().map(|chunk| env.blobs().put(chunk));
        let chunks = futures::future::try_join_all(puts)
            .await
            .map_err(|e| FacetError(format!("sql checkpoint put: {e:?}")))?;
        // Keep the new checkpoint's chunks alive alongside the prior
        // roots (see `SqlDb::roots`): the composite may not commit.
        db.roots
            .lock()
            .expect("sql roots lock")
            .extend(chunks.iter().copied());
        let manifest = SqlManifest {
            page_size: PAGE_SIZE,
            db_bytes,
            chunk_bytes: CHUNK_BYTES as u32,
            chunks,
        };
        Ok(crate::facet::encode_payload(&manifest))
    }

    async fn restore(part: Option<&[u8]>, env: &FacetEnv) -> Result<SqlForm, FacetError> {
        let path = env.scratch_path("db");
        if let Some(dir) = path.parent() {
            fs::create_dir_all(dir).map_err(io_facet_err)?;
        }
        let db = SqlDb {
            path,
            conn: Mutex::new(None),
            in_txn: Mutex::new(false),
            tail: Mutex::new(0),
            roots: Mutex::new(Vec::new()),
        };
        // Drop any stale local cache before materializing: the manifest +
        // committed frame records are the truth (§1); the prior
        // activation's files are not trusted.
        db.delete_files();
        if let Some(bytes) = part {
            let manifest: SqlManifest = crate::facet::decode_payload("sql restore", bytes)?;
            if manifest.page_size != PAGE_SIZE {
                return Err(FacetError(format!(
                    "sql: manifest page size {}, facet fixes {PAGE_SIZE}",
                    manifest.page_size
                )));
            }
            // The chunk fetches are independent; issue them concurrently.
            let gets = manifest.chunks.iter().map(|id| env.blobs().get(*id, None));
            let parts = futures::future::try_join_all(gets)
                .await
                .map_err(|e| FacetError(format!("sql checkpoint get: {e:?}")))?;
            let mut image = parts.concat();
            image.truncate(manifest.db_bytes as usize);
            *db.roots.lock().expect("sql roots lock") = manifest.chunks;
            if !image.is_empty() {
                fs::write(&db.path, &image).map_err(io_facet_err)?;
            }
        }
        Ok(SqlForm(Some(Arc::new(db))))
    }

    fn roots(form: &SqlForm) -> std::collections::BTreeSet<BlobId> {
        match &form.0 {
            Some(db) => db
                .roots
                .lock()
                .expect("sql roots lock")
                .iter()
                .copied()
                .collect(),
            None => std::collections::BTreeSet::new(),
        }
    }
}

/// The handler-facing SQL accessor (spec §7.14), obtained from
/// [`GrainCtx::sql`](crate::GrainCtx::sql). Statements run inside the command's
/// implicit transaction: reads see the transaction's own writes, and the
/// transaction's frames commit atomically with the rest of the command's batch
/// (G19) — or are discarded with the materialization (G20).
pub struct SqlHandle<'a, G: Grain, I>
where
    G::Facets: HasFacet<Sql, I>,
{
    cell: &'a Arc<FacetCell<G::Facets>>,
    _index: std::marker::PhantomData<I>,
}

impl<G: Grain> GrainCtx<G> {
    /// The grain's private SQLite database (spec §7.14). Compiles exactly when
    /// the grain declares the [`Sql`] facet (`type Facets = (Sql, …)`) — the
    /// G10 discipline applied to storage. Valid only inside a command handler:
    /// the per-command transaction is the only capture window, so out-of-command
    /// statements are refused rather than silently un-journaled.
    pub fn sql<I>(&self) -> SqlHandle<'_, G, I>
    where
        G::Facets: HasFacet<Sql, I>,
    {
        SqlHandle {
            cell: self.facet_cell(),
            _index: std::marker::PhantomData,
        }
    }
}

impl<G: Grain, I> SqlHandle<'_, G, I>
where
    G::Facets: HasFacet<Sql, I>,
{
    fn with_conn<R>(
        &self,
        run: impl FnOnce(&Connection) -> Result<R, SqlError>,
    ) -> Result<R, SqlError> {
        self.cell.with_form::<Sql, I, _>(|form| {
            let db = form
                .0
                .as_ref()
                .ok_or_else(|| SqlError("database not materialized".into()))?;
            if !*db.in_txn.lock().expect("sql txn lock") {
                return Err(SqlError(
                    "sql statements are only valid inside a command handler (spec §7.14)".into(),
                ));
            }
            let conn = db.conn.lock().expect("sql conn lock");
            let conn = conn
                .as_ref()
                .ok_or_else(|| SqlError("connection closed".into()))?;
            run(conn)
        })
    }

    /// Execute one statement (DDL or DML), returning the affected row count.
    /// Runs inside the command's transaction; do not issue `BEGIN`/`COMMIT`
    /// yourself (the facet owns the transaction boundary, §7.14).
    pub fn execute(&self, sql: &str, params: &[SqlValue]) -> Result<usize, SqlError> {
        self.with_conn(|conn| {
            conn.execute(sql, rusqlite::params_from_iter(params.iter()))
                .map_err(|e| SqlError(e.to_string()))
        })
    }

    /// Run a query, returning all rows. Sees the transaction's own writes.
    pub fn query(&self, sql: &str, params: &[SqlValue]) -> Result<Vec<SqlRow>, SqlError> {
        self.with_conn(|conn| {
            let mut stmt = conn.prepare(sql).map_err(|e| SqlError(e.to_string()))?;
            let columns = stmt.column_count();
            let mut rows = stmt
                .query(rusqlite::params_from_iter(params.iter()))
                .map_err(|e| SqlError(e.to_string()))?;
            let mut out = Vec::new();
            while let Some(row) = rows.next().map_err(|e| SqlError(e.to_string()))? {
                let mut values = Vec::with_capacity(columns);
                for i in 0..columns {
                    let value = row.get_ref(i).map_err(|e| SqlError(e.to_string()))?;
                    values.push(SqlValue::from(value));
                }
                out.push(values);
            }
            Ok(out)
        })
    }

    /// Run a query expected to return exactly one row.
    pub fn query_one(&self, sql: &str, params: &[SqlValue]) -> Result<SqlRow, SqlError> {
        let mut rows = self.query(sql, params)?;
        match (rows.len(), rows.pop()) {
            (1, Some(row)) => Ok(row),
            (n, _) => Err(SqlError(format!("expected one row, got {n}"))),
        }
    }
}

fn sql_facet_err(e: rusqlite::Error) -> FacetError {
    FacetError(format!("sql: {e}"))
}

fn io_facet_err(e: std::io::Error) -> FacetError {
    FacetError(format!("sql io: {e}"))
}
