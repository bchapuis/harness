//! The ACP front-end: a stdio JSON-RPC bridge that lets any Agent Client
//! Protocol editor (Zed, and the growing set of ACP clients) drive a node.
//!
//! Peer of `repl.rs`, not a model seam: it is a *client-facing* surface, so it
//! reuses `proto.rs` as its backend exactly as the REPL does —
//!
//! ```text
//! editor (ACP client) --stdio JSON-RPC--> harness-standalone acp --TCP proto--> node
//! ```
//!
//! The editor spawns this process and exchanges newline-delimited JSON-RPC 2.0
//! over stdin/stdout (ACP's framing). We translate its session methods to the
//! node's `Op`/`Reply`, and — the one piece of real logic — stream a parked
//! `Prompt` to the editor as `session/update` notifications by polling `Tail`
//! while the run is in flight (`proto.rs` already multiplexes requests by id,
//! the same property that lets the REPL `:tail` a parked prompt).
//!
//! Two deliberate divergences from a typical ACP agent: tools run *inside* the
//! node's sandbox, so we never call back for `fs/*` or `terminal/*`; and the
//! harness auto-runs tools, so there is no `session/request_permission`. What
//! we do offer that most agents cannot is `session/load` — the durable journal
//! *is* the replayable conversation.

use std::collections::HashMap;
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::atomic::AtomicU64;
use std::sync::atomic::Ordering;

use harness::Record;
use harness::RecordBody;
use harness::RunError;
use harness::RunOutcome;
use serde_json::Value;
use serde_json::json;
use tokio::io::AsyncBufReadExt;
use tokio::io::AsyncWriteExt;
use tokio::io::BufReader;
use tokio::net::TcpStream;
use tokio::net::tcp::OwnedWriteHalf;
use tokio::sync::Mutex as AsyncMutex;
use tokio::sync::mpsc;
use tokio::sync::oneshot;

use crate::proto::Op;
use crate::proto::Reply;
use crate::proto::Request;
use crate::proto::Response;

/// The ACP protocol version this adapter speaks.
const PROTOCOL_VERSION: u32 = 1;
/// How long a parked `Prompt` waits — matches the node's submit deadline.
const PROMPT_SECS: u64 = 600;
/// Journal page size for tail-based replay (`session/load`).
const TAIL_PAGE: u32 = 500;

/// Run the adapter against a node's control port until stdin closes.
pub async fn run(control_addr: &str) -> Result<(), String> {
    let backend = Backend::connect(control_addr).await?;

    // One writer task serializes every JSON-RPC frame to stdout, so concurrent
    // notifications and responses never interleave mid-line.
    let (out_tx, mut out_rx) = mpsc::channel::<Value>(64);
    tokio::spawn(async move {
        let mut stdout = tokio::io::stdout();
        while let Some(frame) = out_rx.recv().await {
            let Ok(mut line) = serde_json::to_vec(&frame) else {
                continue;
            };
            line.push(b'\n');
            if stdout.write_all(&line).await.is_err() {
                break;
            }
            let _ = stdout.flush().await;
        }
    });

    let conn = Arc::new(Conn {
        backend,
        out: out_tx,
        in_flight: Mutex::new(HashMap::new()),
    });

    let mut stdin = BufReader::new(tokio::io::stdin()).lines();
    while let Some(line) = stdin.next_line().await.map_err(|e| format!("stdin: {e}"))? {
        if line.trim().is_empty() {
            continue;
        }
        let msg: Value = match serde_json::from_str(&line) {
            Ok(value) => value,
            Err(e) => {
                // No id to correlate a parse failure to; report with a null id.
                let _ = conn
                    .out
                    .send(rpc_error(Value::Null, -32700, &format!("parse error: {e}")))
                    .await;
                continue;
            }
        };
        // Dispatch each message concurrently: a parked `session/prompt` must
        // not block a `session/cancel` or another session's prompt.
        let conn = conn.clone();
        tokio::spawn(async move { dispatch(conn, msg).await });
    }
    Ok(())
}

/// Shared state for one editor connection.
struct Conn {
    backend: Backend,
    out: mpsc::Sender<Value>,
    /// ACP `sessionId` -> the turn id of its in-flight run, for `session/cancel`
    /// (ACP cancels by session; the node cancels by turn).
    in_flight: Mutex<HashMap<String, String>>,
}

/// Route one inbound JSON-RPC message and, if it was a request, reply.
async fn dispatch(conn: Arc<Conn>, msg: Value) {
    let method = msg
        .get("method")
        .and_then(Value::as_str)
        .unwrap_or_default()
        .to_string();
    let id = msg.get("id").cloned();
    let params = msg.get("params").cloned().unwrap_or(Value::Null);

    // A notification — no response, ever.
    if method == "session/cancel" {
        session_cancel(&conn, &params).await;
        return;
    }

    let result = match method.as_str() {
        "initialize" => Ok(initialize_result()),
        "authenticate" => Ok(json!({})), // no auth for a local control port
        "session/new" => session_new(&conn, &params).await,
        "session/load" => session_load(&conn, &params).await,
        "session/prompt" => session_prompt(&conn, &params).await,
        other => Err(RpcErr::method_not_found(other)),
    };

    if let Some(id) = id {
        let frame = match result {
            Ok(value) => rpc_result(id, value),
            Err(err) => rpc_error(id, err.code, &err.message),
        };
        let _ = conn.out.send(frame).await;
    }
}

/// `initialize`: announce the version and the one capability we genuinely add.
fn initialize_result() -> Value {
    json!({
        "protocolVersion": PROTOCOL_VERSION,
        "agentCapabilities": { "loadSession": true },
        "authMethods": [],
    })
}

/// `session/new`: mint an id. The grain is created lazily on the first prompt,
/// so this touches no backend. `cwd`/`mcpServers` are ignored — the sandbox is
/// the node's, server-side.
async fn session_new(_conn: &Conn, _params: &Value) -> Result<Value, RpcErr> {
    let kind = "assistant";
    let session = format!("acp-{}", unique_suffix());
    Ok(json!({ "sessionId": encode_session(kind, &session) }))
}

/// `session/load`: replay the durable journal as `session/update`
/// notifications, then return. The whole reason ACP wants `loadSession`, and
/// the thing the journal makes free.
async fn session_load(conn: &Conn, params: &Value) -> Result<Value, RpcErr> {
    let sid = session_id_param(params)?;
    let (kind, session) = decode_session(&sid)?;
    let _ = stream_since(conn, &sid, &kind, &session, 0).await?;
    Ok(json!({}))
}

/// `session/prompt`: submit a turn and stream its records, returning the ACP
/// stop reason once the run reaches its terminal outcome.
async fn session_prompt(conn: &Conn, params: &Value) -> Result<Value, RpcErr> {
    let sid = session_id_param(params)?;
    let (kind, session) = decode_session(&sid)?;
    let content = flatten_prompt(params);
    // Derive the next turn id from the journal, exactly as the REPL does, so a
    // reconnected editor never collides an id (H7 would dedup it anyway).
    let turn = next_turn_id(&conn.backend, &kind, &session).await?;

    conn.in_flight
        .lock()
        .unwrap()
        .insert(sid.clone(), turn.clone());
    let outcome = prompt_streaming(conn, &sid, &kind, &session, &turn, content).await;
    conn.in_flight.lock().unwrap().remove(&sid);

    match outcome? {
        Ok(_) => Ok(json!({ "stopReason": "end_turn" })),
        Err(RunError::BudgetExhausted) => Ok(json!({ "stopReason": "max_tokens" })),
        Err(RunError::Cancelled) => Ok(json!({ "stopReason": "cancelled" })),
        // A model failure is no kind of clean stop; surface it as an error.
        Err(RunError::Model(e)) => Err(RpcErr::internal(format!("model failure: {e}"))),
    }
}

/// `session/cancel` (notification): cancel the session's in-flight run by the
/// turn id we recorded when it was submitted.
async fn session_cancel(conn: &Conn, params: &Value) {
    let Some(sid) = params.get("sessionId").and_then(Value::as_str) else {
        return;
    };
    let turn = conn.in_flight.lock().unwrap().get(sid).cloned();
    if let (Some(turn), Ok((kind, session))) = (turn, decode_session(sid)) {
        let _ = conn
            .backend
            .call(Op::Cancel {
                kind,
                session,
                turn,
            })
            .await;
    }
}

/// The streaming bridge: open a `Watch` on the run, park the `Prompt`, and
/// forward each pushed `Records` frame as `session/update` notifications until
/// the run reaches its terminal outcome. The node pushes on its harness-event
/// stream — no polling. `proto.rs`'s id multiplexing lets the watch and the
/// parked prompt share one connection.
async fn prompt_streaming(
    conn: &Conn,
    sid: &str,
    kind: &str,
    session: &str,
    turn: &str,
    content: String,
) -> Result<RunOutcome, RpcErr> {
    // Stream only this turn's records: watch from the journal's current end.
    let from = journal_len(&conn.backend, kind, session).await?;
    let mut watch = conn
        .backend
        .watch(Op::Watch {
            kind: kind.to_string(),
            session: session.to_string(),
            turn: turn.to_string(),
            from,
        })
        .await?;
    let prompt = conn.backend.call(Op::Prompt {
        kind: kind.to_string(),
        session: session.to_string(),
        turn: turn.to_string(),
        content,
        within_secs: PROMPT_SECS,
    });
    tokio::pin!(prompt);
    let mut watching = true;
    loop {
        tokio::select! {
            reply = &mut prompt => {
                // The run is done; flush frames already queued on the watch so
                // every record is emitted before the stop reason.
                while let Ok(frame) = watch.try_recv() {
                    emit_frame(conn, sid, frame).await;
                }
                return match reply? {
                    Reply::Outcome { outcome } => Ok(outcome),
                    Reply::Error { message } => Err(RpcErr::internal(message)),
                    _ => Err(RpcErr::internal("unexpected reply to prompt".to_string())),
                };
            }
            frame = watch.recv(), if watching => {
                match frame {
                    Some(frame) => emit_frame(conn, sid, frame).await,
                    None => watching = false, // watch ended or the stream closed
                }
            }
        }
    }
}

/// Forward one watch frame to the editor as `session/update` notifications.
async fn emit_frame(conn: &Conn, sid: &str, frame: Reply) {
    if let Reply::Records { records } = frame {
        for (_, record) in &records {
            for update in record_updates(sid, record) {
                let _ = conn.out.send(rpc_notify("session/update", update)).await;
            }
        }
    }
}

/// Emit a `session/update` for every record after `from` (a one-shot replay for
/// `session/load`), returning the new high-water seq.
async fn stream_since(
    conn: &Conn,
    sid: &str,
    kind: &str,
    session: &str,
    mut from: u64,
) -> Result<u64, RpcErr> {
    loop {
        let records = tail(&conn.backend, kind, session, from).await?;
        if records.is_empty() {
            break;
        }
        for (seq, record) in &records {
            for update in record_updates(sid, record) {
                let _ = conn.out.send(rpc_notify("session/update", update)).await;
            }
            from = seq.value();
        }
        if records.len() < TAIL_PAGE as usize {
            break;
        }
    }
    Ok(from)
}

/// The current end of a session's journal (its last committed seq, or 0).
async fn journal_len(backend: &Backend, kind: &str, session: &str) -> Result<u64, RpcErr> {
    let mut from = 0;
    loop {
        let records = tail(backend, kind, session, from).await?;
        if records.is_empty() {
            break;
        }
        from = records.last().map(|(seq, _)| seq.value()).unwrap_or(from);
        if records.len() < TAIL_PAGE as usize {
            break;
        }
    }
    Ok(from)
}

/// The next turn id, derived by counting submitted turns — the REPL's
/// `seed_turns` logic, so the two front-ends allocate ids the same way.
async fn next_turn_id(backend: &Backend, kind: &str, session: &str) -> Result<String, RpcErr> {
    let mut turns = 0u64;
    let mut from = 0;
    loop {
        let records = tail(backend, kind, session, from).await?;
        if records.is_empty() {
            break;
        }
        turns += records
            .iter()
            .filter(|(_, r)| matches!(r.body, RecordBody::TurnSubmitted { .. }))
            .count() as u64;
        from = records.last().map(|(seq, _)| seq.value()).unwrap_or(from);
        if records.len() < TAIL_PAGE as usize {
            break;
        }
    }
    Ok(format!("t-{}", turns + 1))
}

/// One tail page, with the protocol-level replies folded into an error.
async fn tail(
    backend: &Backend,
    kind: &str,
    session: &str,
    from: u64,
) -> Result<Vec<(harness::Seq, Record)>, RpcErr> {
    match backend
        .call(Op::Tail {
            kind: kind.to_string(),
            session: session.to_string(),
            from,
            limit: TAIL_PAGE,
        })
        .await?
    {
        Reply::Records { records } => Ok(records),
        Reply::Error { message } => Err(RpcErr::internal(message)),
        _ => Err(RpcErr::internal("unexpected reply to tail".to_string())),
    }
}

/// Translate one journal record into zero or more ACP `session/update` payloads.
/// Mirrors `repl::render`'s record taxonomy, in ACP's notification shapes.
fn record_updates(sid: &str, record: &Record) -> Vec<Value> {
    let wrap = |update: Value| json!({ "sessionId": sid, "update": update });
    match &record.body {
        RecordBody::TurnSubmitted { content, .. } => vec![wrap(json!({
            "sessionUpdate": "user_message_chunk",
            "content": { "type": "text", "text": content },
        }))],
        RecordBody::ModelResponse { content, calls, .. } => {
            let mut updates = Vec::new();
            if !content.is_empty() {
                updates.push(wrap(json!({
                    "sessionUpdate": "agent_message_chunk",
                    "content": { "type": "text", "text": content },
                })));
            }
            for call in calls {
                updates.push(wrap(json!({
                    "sessionUpdate": "tool_call",
                    "toolCallId": call.id.as_str(),
                    "title": call.name,
                    "status": "pending",
                    "rawInput": call.input,
                })));
            }
            updates
        }
        RecordBody::ToolOutcome { call, outcome, .. } => {
            let (status, text) = match outcome {
                Ok(value) => ("completed", value.to_string()),
                Err(error) => ("failed", format!("{error}")),
            };
            vec![wrap(json!({
                "sessionUpdate": "tool_call_update",
                "toolCallId": call.as_str(),
                "status": status,
                "content": [ { "type": "content", "content": { "type": "text", "text": text } } ],
            }))]
        }
        RecordBody::ChildRun {
            call,
            child_session,
            ..
        } => vec![wrap(json!({
            "sessionUpdate": "tool_call",
            "toolCallId": call.as_str(),
            "title": format!("delegate \u{2192} {}", child_session.as_str()),
            "status": "pending",
        }))],
        // SessionCreated, WorkspaceReset, TierAcquired, RunEnded carry nothing
        // the editor renders as a turn; the stop reason conveys the ending.
        _ => Vec::new(),
    }
}

/// Flatten an ACP prompt (an array of content blocks) to the plain text the
/// node's `content: String` takes; non-text blocks are dropped for now.
fn flatten_prompt(params: &Value) -> String {
    params
        .get("prompt")
        .and_then(Value::as_array)
        .map(|blocks| {
            blocks
                .iter()
                .filter(|b| b.get("type").and_then(Value::as_str) == Some("text"))
                .filter_map(|b| b.get("text").and_then(Value::as_str))
                .collect::<Vec<_>>()
                .join("")
        })
        .unwrap_or_default()
}

/// Encode the harness `(kind, session)` pair into one opaque ACP session id.
/// `kind` never contains `/`, so the first `/` splits it back unambiguously
/// even when `session` is a delegated child id like `parent/turn/call`.
fn encode_session(kind: &str, session: &str) -> String {
    format!("{kind}/{session}")
}

fn decode_session(sid: &str) -> Result<(String, String), RpcErr> {
    sid.split_once('/')
        .map(|(kind, session)| (kind.to_string(), session.to_string()))
        .ok_or_else(|| RpcErr::invalid_params("sessionId must be `kind/session`"))
}

fn session_id_param(params: &Value) -> Result<String, RpcErr> {
    params
        .get("sessionId")
        .and_then(Value::as_str)
        .map(str::to_string)
        .ok_or_else(|| RpcErr::invalid_params("missing sessionId"))
}

/// A collision-resistant suffix for a freshly minted session name. The wall
/// clock is off-limits (the determinism guard-rail), so we combine this
/// process's pid with a monotonic counter — unique across concurrent adapters
/// and across `session/new` calls within one, without reading time.
fn unique_suffix() -> String {
    static COUNTER: AtomicU64 = AtomicU64::new(0);
    let n = COUNTER.fetch_add(1, Ordering::Relaxed);
    format!("{}-{}", std::process::id(), n)
}

/// The TCP control connection to the node, with id-correlated multiplexing so
/// concurrent requests (a parked prompt and its live watch) share one socket.
/// Single-reply ops resolve through `pending`; a `Watch` streams many replies
/// to its `watchers` channel until `WatchEnded`.
struct Backend {
    write: AsyncMutex<OwnedWriteHalf>,
    pending: Arc<Mutex<HashMap<u64, oneshot::Sender<Reply>>>>,
    watchers: Arc<Mutex<HashMap<u64, mpsc::Sender<Reply>>>>,
    next_id: AtomicU64,
}

impl Backend {
    async fn connect(addr: &str) -> Result<Backend, String> {
        let stream = TcpStream::connect(addr)
            .await
            .map_err(|e| format!("connect {addr}: {e} (is the node running?)"))?;
        let (read, write) = stream.into_split();
        let pending: Arc<Mutex<HashMap<u64, oneshot::Sender<Reply>>>> = Arc::default();
        let watchers: Arc<Mutex<HashMap<u64, mpsc::Sender<Reply>>>> = Arc::default();
        let reader_pending = pending.clone();
        let reader_watchers = watchers.clone();
        tokio::spawn(async move {
            let mut lines = BufReader::new(read).lines();
            while let Ok(Some(line)) = lines.next_line().await {
                if line.trim().is_empty() {
                    continue;
                }
                let Ok(response) = serde_json::from_str::<Response>(&line) else {
                    continue;
                };
                // A single-reply op claims its waiter and is done.
                if let Some(tx) = reader_pending.lock().unwrap().remove(&response.id) {
                    let _ = tx.send(response.body);
                    continue;
                }
                // Otherwise a watch frame: forward it, and retire the watch once
                // its terminal frame lands.
                let ended = matches!(response.body, Reply::WatchEnded);
                let sender = reader_watchers.lock().unwrap().get(&response.id).cloned();
                if let Some(sender) = sender {
                    let _ = sender.send(response.body).await;
                }
                if ended {
                    reader_watchers.lock().unwrap().remove(&response.id);
                }
            }
            // The node hung up: drop every waiter so its `call`/`watch` ends.
            reader_pending.lock().unwrap().clear();
            reader_watchers.lock().unwrap().clear();
        });
        Ok(Backend {
            write: AsyncMutex::new(write),
            pending,
            watchers,
            next_id: AtomicU64::new(1),
        })
    }

    async fn send(&self, id: u64, op: Op) -> Result<(), RpcErr> {
        let mut line = serde_json::to_vec(&Request { id, op })
            .map_err(|e| RpcErr::internal(format!("encode: {e}")))?;
        line.push(b'\n');
        self.write
            .lock()
            .await
            .write_all(&line)
            .await
            .map_err(|e| RpcErr::internal(format!("send: {e}")))
    }

    async fn call(&self, op: Op) -> Result<Reply, RpcErr> {
        let id = self.next_id.fetch_add(1, Ordering::Relaxed);
        let (tx, rx) = oneshot::channel();
        self.pending.lock().unwrap().insert(id, tx);
        if let Err(e) = self.send(id, op).await {
            self.pending.lock().unwrap().remove(&id);
            return Err(e);
        }
        rx.await
            .map_err(|_| RpcErr::internal("the node closed the connection".to_string()))
    }

    /// Open a `Watch`: the returned channel yields each streamed `Records`
    /// frame, then closes once the node sends `WatchEnded` (or the connection
    /// drops).
    async fn watch(&self, op: Op) -> Result<mpsc::Receiver<Reply>, RpcErr> {
        let id = self.next_id.fetch_add(1, Ordering::Relaxed);
        let (tx, rx) = mpsc::channel::<Reply>(64);
        self.watchers.lock().unwrap().insert(id, tx);
        if let Err(e) = self.send(id, op).await {
            self.watchers.lock().unwrap().remove(&id);
            return Err(e);
        }
        Ok(rx)
    }
}

/// A JSON-RPC error, carried out of a handler.
#[derive(Debug)]
struct RpcErr {
    code: i64,
    message: String,
}

impl RpcErr {
    fn internal(message: String) -> RpcErr {
        RpcErr {
            code: -32603,
            message,
        }
    }
    fn method_not_found(method: &str) -> RpcErr {
        RpcErr {
            code: -32601,
            message: format!("method not found: {method}"),
        }
    }
    fn invalid_params(message: &str) -> RpcErr {
        RpcErr {
            code: -32602,
            message: message.to_string(),
        }
    }
}

fn rpc_result(id: Value, result: Value) -> Value {
    json!({ "jsonrpc": "2.0", "id": id, "result": result })
}

fn rpc_error(id: Value, code: i64, message: &str) -> Value {
    json!({ "jsonrpc": "2.0", "id": id, "error": { "code": code, "message": message } })
}

fn rpc_notify(method: &str, params: Value) -> Value {
    json!({ "jsonrpc": "2.0", "method": method, "params": params })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn session_id_round_trips_through_a_delegated_child() {
        // `kind` has no `/`, so the first `/` splits cleanly even when the
        // session is a delegated child id that itself contains `/`.
        let sid = encode_session("assistant", "parent/t-1/tu_42");
        assert_eq!(
            decode_session(&sid).unwrap(),
            ("assistant".to_string(), "parent/t-1/tu_42".to_string())
        );
    }

    #[test]
    fn decode_rejects_an_id_without_a_kind() {
        assert!(decode_session("no-slash").is_err());
    }

    #[test]
    fn flatten_keeps_text_blocks_and_drops_the_rest() {
        let params = json!({
            "prompt": [
                { "type": "text", "text": "hello " },
                { "type": "image", "data": "…" },
                { "type": "text", "text": "world" },
            ]
        });
        assert_eq!(flatten_prompt(&params), "hello world");
    }

    #[test]
    fn a_model_response_maps_to_a_chunk_and_a_tool_call() {
        let record = Record {
            at_nanos: 0,
            body: RecordBody::ModelResponse {
                turn: harness::TurnId::new("t-1"),
                content: "thinking".to_string(),
                calls: vec![harness::ToolCall {
                    id: harness::CallId::new("c-1"),
                    name: "shell".to_string(),
                    input: json!({ "command": "ls" }),
                }],
                usage: harness::Usage::default(),
            },
        };
        let updates = record_updates("assistant/demo", &record);
        assert_eq!(updates.len(), 2);
        assert_eq!(updates[0]["update"]["sessionUpdate"], "agent_message_chunk");
        assert_eq!(updates[1]["update"]["sessionUpdate"], "tool_call");
        assert_eq!(updates[1]["update"]["toolCallId"], "c-1");
    }
}
