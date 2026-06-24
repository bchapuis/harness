//! Shared test scaffolding for the ACP adapter: a fake control-port node and a
//! handful of record builders.
//!
//! The adapter (`acp::serve`) is a translator between ACP JSON-RPC and the
//! node's `proto` control protocol. To test the translation we do not need a
//! real cluster, model, or sandbox — only something that speaks `proto` on a TCP
//! port and returns scripted replies. [`FakeNode`] is that: it reads
//! `proto::Request` lines and answers each op with canned `proto::Reply` frames,
//! exactly as the real node's control listener would (`node::serve_connection`).
//!
//! A [`Scenario`] is the script. It separates the *committed history* (what
//! `Tail` and `session/load` replay) from the *turn* (the records a
//! `session/prompt` streams over its `Watch`) and the run's terminal `outcome`
//! (what the parked `Prompt` resolves to). That mirrors how the adapter drives a
//! turn: derive the cursor from the journal, open a watch, park the prompt.

#![allow(dead_code)] // each integration test binary uses a different subset

use std::net::SocketAddr;
use std::sync::Arc;

use harness::CallId;
use harness::Completion;
use harness::Record;
use harness::RecordBody;
use harness::RunError;
use harness::RunOutcome;
use harness::Seq;
use harness::ToolCall;
use harness::ToolError;
use harness::TurnId;
use harness::Usage;
use harness_standalone::proto::Op;
use harness_standalone::proto::Reply;
use harness_standalone::proto::Request;
use harness_standalone::proto::Response;
use serde_json::Value;
use tokio::io::AsyncBufReadExt;
use tokio::io::AsyncWriteExt;
use tokio::io::BufReader;
use tokio::net::TcpListener;
use tokio::net::TcpStream;
use tokio::sync::Notify;
use tokio::sync::mpsc;

/// A scripted control-port backend.
#[derive(Clone)]
pub struct Scenario {
    /// Records already committed before this connection: what `Tail` pages over
    /// and what `session/load` replays. `Seq` is `index + 1`, the node's rule.
    history: Arc<Vec<Record>>,
    /// The records a `session/prompt` streams over its `Watch`, in order. Their
    /// `Seq`s continue after `history`.
    turn: Arc<Vec<Record>>,
    /// The terminal outcome the parked `Prompt` resolves to.
    outcome: Arc<RunOutcome>,
    /// When set, the `Prompt` does not resolve until a `Cancel` op arrives, and
    /// then resolves to `Err(Cancelled)` — the cancel handshake the adapter
    /// turns into a `cancelled` stop reason.
    block_until_cancel: bool,
}

impl Scenario {
    pub fn new() -> Scenario {
        Scenario {
            history: Arc::new(Vec::new()),
            turn: Arc::new(Vec::new()),
            outcome: Arc::new(Ok(Completion::new("done", 0))),
            block_until_cancel: false,
        }
    }

    /// Pre-existing committed records (for `Tail` / `session/load`).
    pub fn history(mut self, records: Vec<Record>) -> Scenario {
        self.history = Arc::new(records);
        self
    }

    /// The records a prompt streams, and the run's terminal outcome.
    pub fn turn(mut self, records: Vec<Record>, outcome: RunOutcome) -> Scenario {
        self.turn = Arc::new(records);
        self.outcome = Arc::new(outcome);
        self
    }

    /// Park the prompt until a cancel arrives; then resolve cancelled.
    pub fn cancellable(mut self, records: Vec<Record>) -> Scenario {
        self.turn = Arc::new(records);
        self.outcome = Arc::new(Err(RunError::Cancelled));
        self.block_until_cancel = true;
        self
    }

    /// One `Tail` page: the history records with `Seq > from`, capped at `limit`.
    fn tail(&self, from: u64, limit: u32) -> Vec<(Seq, Record)> {
        self.history
            .iter()
            .enumerate()
            .map(|(i, r)| (Seq::new(i as u64 + 1), r.clone()))
            .filter(|(seq, _)| seq.value() > from)
            .take(limit as usize)
            .collect()
    }

    /// The turn's records, `Seq`d continuing after history — what a `Watch`
    /// streams.
    fn turn_page(&self) -> Vec<(Seq, Record)> {
        let base = self.history.len() as u64;
        self.turn
            .iter()
            .enumerate()
            .map(|(i, r)| (Seq::new(base + i as u64 + 1), r.clone()))
            .collect()
    }
}

/// A running fake node. Drop it to stop serving.
pub struct FakeNode {
    pub addr: SocketAddr,
    _shutdown: mpsc::Sender<()>,
}

impl FakeNode {
    /// Bind an ephemeral port and serve `scenario` to every connection until
    /// dropped.
    pub async fn start(scenario: Scenario) -> FakeNode {
        let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind fake node");
        let addr = listener.local_addr().expect("addr");
        let (shutdown, mut rx) = mpsc::channel::<()>(1);
        tokio::spawn(async move {
            loop {
                tokio::select! {
                    _ = rx.recv() => break,
                    accepted = listener.accept() => {
                        let Ok((stream, _)) = accepted else { break };
                        tokio::spawn(serve(stream, scenario.clone()));
                    }
                }
            }
        });
        FakeNode {
            addr,
            _shutdown: shutdown,
        }
    }

    pub fn addr(&self) -> String {
        self.addr.to_string()
    }
}

/// One control connection: read `proto::Request` lines, answer each op with the
/// scenario's scripted frames. Ops are handled on spawned tasks so a parked
/// `Prompt` does not block the `Watch` stream — the same concurrency the real
/// `serve_connection` provides.
async fn serve(stream: TcpStream, scenario: Scenario) {
    let (read, mut write) = stream.into_split();
    let (tx, mut out_rx) = mpsc::channel::<Response>(64);
    tokio::spawn(async move {
        while let Some(response) = out_rx.recv().await {
            let Ok(mut line) = serde_json::to_vec(&response) else {
                continue;
            };
            line.push(b'\n');
            if write.write_all(&line).await.is_err() {
                break;
            }
        }
    });
    let cancel = Arc::new(Notify::new());
    let mut lines = BufReader::new(read).lines();
    while let Ok(Some(line)) = lines.next_line().await {
        if line.trim().is_empty() {
            continue;
        }
        let Ok(request) = serde_json::from_str::<Request>(&line) else {
            continue;
        };
        let id = request.id;
        let tx = tx.clone();
        let scenario = scenario.clone();
        let cancel = cancel.clone();
        match request.op {
            Op::Tail { from, limit, .. } => {
                let records = scenario.tail(from, limit);
                let _ = tx.send(reply(id, Reply::Records { records })).await;
            }
            Op::Watch { .. } => {
                tokio::spawn(async move {
                    let records = scenario.turn_page();
                    if !records.is_empty() {
                        let _ = tx.send(reply(id, Reply::Records { records })).await;
                    }
                    let _ = tx.send(reply(id, Reply::WatchEnded)).await;
                });
            }
            Op::Prompt { .. } => {
                tokio::spawn(async move {
                    if scenario.block_until_cancel {
                        cancel.notified().await;
                    }
                    let outcome = (*scenario.outcome).clone();
                    let _ = tx.send(reply(id, Reply::Outcome { outcome })).await;
                });
            }
            Op::Cancel { .. } => {
                cancel.notify_waiters();
                let _ = tx.send(reply(id, Reply::Cancelled)).await;
            }
        }
    }
}

fn reply(id: u64, body: Reply) -> Response {
    Response { id, body }
}

// ---- record builders ------------------------------------------------------

fn rec(body: RecordBody) -> Record {
    Record { at_nanos: 0, body }
}

pub fn turn_submitted(turn: &str, content: &str) -> Record {
    rec(RecordBody::TurnSubmitted {
        turn: TurnId::new(turn),
        content: content.to_string(),
        budget: harness::Budget::new(200_000, 50),
    })
}

pub fn model_text(turn: &str, content: &str) -> Record {
    rec(RecordBody::ModelResponse {
        turn: TurnId::new(turn),
        content: content.to_string(),
        calls: Vec::new(),
        usage: Usage::default(),
    })
}

pub fn model_tool_call(turn: &str, call_id: &str, name: &str, input: Value) -> Record {
    rec(RecordBody::ModelResponse {
        turn: TurnId::new(turn),
        content: String::new(),
        calls: vec![ToolCall {
            id: CallId::new(call_id),
            name: name.to_string(),
            input,
        }],
        usage: Usage::default(),
    })
}

pub fn tool_ok(turn: &str, call_id: &str, output: Value) -> Record {
    rec(RecordBody::ToolOutcome {
        turn: TurnId::new(turn),
        call: CallId::new(call_id),
        outcome: Ok(output),
    })
}

pub fn tool_err(turn: &str, call_id: &str, message: &str) -> Record {
    rec(RecordBody::ToolOutcome {
        turn: TurnId::new(turn),
        call: CallId::new(call_id),
        outcome: Err(ToolError::Sandbox(message.to_string())),
    })
}

pub fn run_ended_ok(turn: &str, content: &str) -> Record {
    rec(RecordBody::RunEnded {
        turn: TurnId::new(turn),
        outcome: Ok(Completion::new(content, 0)),
    })
}

pub fn completion(content: &str) -> RunOutcome {
    Ok(Completion::new(content, 0))
}
