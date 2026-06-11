//! The REPL: a line-oriented client over the control protocol.
//!
//! A plain line submits a turn to the current session; `:`-commands switch
//! sessions, read the journal, cancel, and — the failure-demo move —
//! `:retry`, which re-submits the **same** turn id so a run orphaned by a
//! killed node is re-attached wherever the session lives now (caller-driven
//! resumption, harness spec §7.5, invariant H7).
//!
//! Requests and responses are correlated by id, so a parked prompt never
//! blocks `:tail` or `:cancel` — and slow outcomes print whenever they land.

use std::collections::HashMap;
use std::io::Write as _;

use harness::Record;
use harness::RecordBody;
use harness::RunOutcome;
use harness::SeqNo;
use tokio::io::AsyncBufReadExt;
use tokio::io::AsyncWriteExt;
use tokio::io::BufReader;
use tokio::io::Lines;
use tokio::net::TcpStream;
use tokio::net::tcp::OwnedReadHalf;
use tokio::net::tcp::OwnedWriteHalf;

use crate::proto::Op;
use crate::proto::Reply;
use crate::proto::Request;
use crate::proto::Response;

const HELP: &str = "\
  <text>          submit the text as the session's next turn
  :retry          re-submit the last turn id (re-attach after a failure)
  :cancel         cancel the last submitted turn
  :tail           print the session's journal
  :session <id>   switch session (created on its first turn)
  :kind <name>    switch kind (assistant | worker)
  :help           this help
  :quit           leave (the cluster and its sessions keep running)";

/// What the prompt waits on by default: matches the node's submit deadline.
const PROMPT_SECS: u64 = 600;
/// Journal page size for :tail and turn-counter seeding.
const PAGE: u32 = 500;

pub async fn run(addr: &str) -> Result<(), String> {
    let stream = TcpStream::connect(addr)
        .await
        .map_err(|e| format!("connect {addr}: {e} (is the node running?)"))?;
    let (read, write) = stream.into_split();
    let mut server = BufReader::new(read).lines();
    let mut stdin = BufReader::new(tokio::io::stdin()).lines();
    let mut repl = Repl {
        write,
        kind: "assistant".to_string(),
        session: "demo".to_string(),
        next_id: 1,
        next_turn: 1,
        last: None,
        pending: HashMap::new(),
    };
    println!("attached to {addr}");
    println!("{HELP}");
    repl.seed_turns(&mut server).await?;
    repl.show_prompt();
    loop {
        tokio::select! {
            line = stdin.next_line() => {
                let Some(line) = line.map_err(|e| format!("stdin: {e}"))? else {
                    return Ok(()); // EOF
                };
                if !repl.input(line.trim(), &mut server).await? {
                    return Ok(());
                }
                repl.show_prompt();
            }
            line = server.next_line() => {
                let Some(line) = line.map_err(|e| format!("server: {e}"))? else {
                    return Err("the node closed the connection".to_string());
                };
                println!();
                repl.server_line(&line);
                repl.show_prompt();
            }
        }
    }
}

/// What a pending request id resolves to, for printing.
enum PendingOp {
    Prompt { turn: String },
    Tail,
    Cancel { turn: String },
}

struct Repl {
    write: OwnedWriteHalf,
    kind: String,
    session: String,
    next_id: u64,
    next_turn: u64,
    /// The last submitted turn: `(turn id, content)`, what :retry re-sends.
    last: Option<(String, String)>,
    pending: HashMap<u64, PendingOp>,
}

type ServerLines = Lines<BufReader<OwnedReadHalf>>;

impl Repl {
    fn show_prompt(&self) {
        print!("{}/{}> ", self.kind, self.session);
        let _ = std::io::stdout().flush();
    }

    /// Handle one input line; `Ok(false)` ends the REPL.
    async fn input(&mut self, line: &str, server: &mut ServerLines) -> Result<bool, String> {
        match line {
            "" => {}
            ":quit" | ":exit" => return Ok(false),
            ":help" => println!("{HELP}"),
            ":tail" => {
                let session = self.session.clone();
                let id = self
                    .send(Op::Tail {
                        kind: self.kind.clone(),
                        session,
                        from: 0,
                        limit: PAGE,
                    })
                    .await?;
                self.pending.insert(id, PendingOp::Tail);
            }
            ":cancel" => match &self.last {
                Some((turn, _)) => {
                    let turn = turn.clone();
                    let id = self
                        .send(Op::Cancel {
                            kind: self.kind.clone(),
                            session: self.session.clone(),
                            turn: turn.clone(),
                        })
                        .await?;
                    self.pending.insert(id, PendingOp::Cancel { turn });
                }
                None => println!("nothing submitted yet"),
            },
            ":retry" => match self.last.clone() {
                Some((turn, content)) => self.submit(turn, content).await?,
                None => println!("nothing to retry yet"),
            },
            _ => {
                if let Some(rest) = line.strip_prefix(":session ") {
                    self.session = rest.trim().to_string();
                    self.last = None;
                    self.seed_turns(server).await?;
                } else if let Some(rest) = line.strip_prefix(":kind ") {
                    self.kind = rest.trim().to_string();
                    self.last = None;
                    self.seed_turns(server).await?;
                } else if line.starts_with(':') {
                    println!("unknown command (:help lists them)");
                } else {
                    let turn = format!("t-{}", self.next_turn);
                    self.next_turn += 1;
                    self.submit(turn, line.to_string()).await?;
                }
            }
        }
        Ok(true)
    }

    async fn submit(&mut self, turn: String, content: String) -> Result<(), String> {
        self.last = Some((turn.clone(), content.clone()));
        let id = self
            .send(Op::Prompt {
                kind: self.kind.clone(),
                session: self.session.clone(),
                turn: turn.clone(),
                content,
                within_secs: PROMPT_SECS,
            })
            .await?;
        println!("· submitted {turn} (waiting for the run; :retry re-attaches after a failure)");
        self.pending.insert(id, PendingOp::Prompt { turn });
        Ok(())
    }

    async fn send(&mut self, op: Op) -> Result<u64, String> {
        let id = self.next_id;
        self.next_id += 1;
        let mut line =
            serde_json::to_vec(&Request { id, op }).map_err(|e| format!("encode: {e}"))?;
        line.push(b'\n');
        self.write
            .write_all(&line)
            .await
            .map_err(|e| format!("send: {e}"))?;
        Ok(id)
    }

    /// Seed the turn counter from the journal, so a REPL restarted against a
    /// durable session never reuses a turn id by accident. (A reused id is
    /// never corruption — it dedups to the recorded outcome, H7 — just
    /// confusing.) Responses to other pending requests are printed as they
    /// stream past.
    async fn seed_turns(&mut self, server: &mut ServerLines) -> Result<(), String> {
        let mut turns: u64 = 0;
        let mut from: u64 = 0;
        loop {
            let id = self
                .send(Op::Tail {
                    kind: self.kind.clone(),
                    session: self.session.clone(),
                    from,
                    limit: PAGE,
                })
                .await?;
            let reply = self.await_reply(id, server).await?;
            let Reply::Records { records } = reply else {
                // A fresh cluster may still be converging; start from 1 and
                // let H7 dedup absorb any collision.
                self.next_turn = 1;
                return Ok(());
            };
            turns += records
                .iter()
                .filter(|(_, r)| matches!(r.body, RecordBody::TurnSubmitted { .. }))
                .count() as u64;
            let full_page = records.len() as u32 == PAGE;
            from = records.last().map(|(seq, _)| seq.0).unwrap_or(from);
            if !full_page {
                break;
            }
        }
        self.next_turn = turns + 1;
        if turns > 0 {
            println!(
                "(session has {turns} prior turns; continuing at t-{})",
                self.next_turn
            );
        }
        Ok(())
    }

    /// Wait for the response to `id`, printing any other responses that
    /// arrive in the meantime.
    async fn await_reply(&mut self, id: u64, server: &mut ServerLines) -> Result<Reply, String> {
        loop {
            let Some(line) = server
                .next_line()
                .await
                .map_err(|e| format!("server: {e}"))?
            else {
                return Err("the node closed the connection".to_string());
            };
            let response: Response =
                serde_json::from_str(&line).map_err(|e| format!("bad response: {e}"))?;
            if response.id == id {
                return Ok(response.body);
            }
            self.print_response(response);
        }
    }

    fn server_line(&mut self, line: &str) {
        match serde_json::from_str::<Response>(line) {
            Ok(response) => self.print_response(response),
            Err(e) => println!("✗ unintelligible response: {e}"),
        }
    }

    fn print_response(&mut self, response: Response) {
        let pending = self.pending.remove(&response.id);
        match (response.body, pending) {
            (Reply::Outcome { outcome }, Some(PendingOp::Prompt { turn })) => {
                print_outcome(&turn, &outcome);
            }
            (Reply::Outcome { outcome }, _) => print_outcome("?", &outcome),
            (Reply::Records { records }, _) => {
                if records.is_empty() {
                    println!("(no records yet)");
                }
                for (seq, record) in &records {
                    println!("{}", render(*seq, record));
                }
            }
            (Reply::Cancelled, Some(PendingOp::Cancel { turn })) => {
                println!("· cancel of {turn} acknowledged");
            }
            (Reply::Cancelled, _) => println!("· cancel acknowledged"),
            (Reply::Error { message }, pending) => {
                let about = match pending {
                    Some(PendingOp::Prompt { turn }) => format!(" ({turn})"),
                    _ => String::new(),
                };
                println!("✗ transport{about}: {message}");
                println!("  the run, if started, continues on the cluster — :retry re-attaches");
            }
        }
    }
}

fn print_outcome(turn: &str, outcome: &RunOutcome) {
    match outcome {
        Ok(completion) => {
            println!("{}", completion.text());
            println!("· {turn} done, {} tokens", completion.tokens);
        }
        Err(error) => println!("✗ {turn} failed: {error}"),
    }
}

/// One journal record, one line — the audit view (harness spec §10.1).
fn render(seq: SeqNo, record: &Record) -> String {
    match &record.body {
        RecordBody::SessionCreated { kind, parent, .. } => {
            let parent = parent
                .as_ref()
                .map(|lineage| format!(" (delegated by {})", lineage.session.as_str()))
                .unwrap_or_default();
            format!("{seq} session created, kind {}{parent}", kind.as_str())
        }
        RecordBody::TurnSubmitted { turn, content, .. } => {
            format!(
                "{seq} turn {} submitted: {}",
                turn.as_str(),
                snippet(content)
            )
        }
        RecordBody::ModelResponse {
            content,
            calls,
            usage,
            ..
        } => {
            let calls = if calls.is_empty() {
                String::new()
            } else {
                format!(
                    " [calls: {}]",
                    calls
                        .iter()
                        .map(|c| c.name.as_str())
                        .collect::<Vec<_>>()
                        .join(", ")
                )
            };
            format!(
                "{seq} model ({} tokens): {}{calls}",
                usage.input_tokens + usage.output_tokens,
                snippet(content)
            )
        }
        RecordBody::ToolOutcome { call, outcome, .. } => match outcome {
            Ok(value) => format!(
                "{seq} tool {} ok: {}",
                call.as_str(),
                snippet(&value.to_string())
            ),
            Err(error) => format!("{seq} tool {} failed: {error}", call.as_str()),
        },
        RecordBody::ChildRun {
            child_session,
            budget,
            ..
        } => format!(
            "{seq} delegated to {} ({} tokens carved)",
            child_session.as_str(),
            budget.tokens
        ),
        RecordBody::WorkspaceReset => format!("{seq} workspace reset"),
        RecordBody::RunEnded { turn, outcome } => match outcome {
            Ok(completion) => format!(
                "{seq} run {} ended ok ({} tokens)",
                turn.as_str(),
                completion.tokens
            ),
            Err(error) => format!("{seq} run {} ended: {error}", turn.as_str()),
        },
    }
}

fn snippet(text: &str) -> String {
    let mut flat = text.split_whitespace().collect::<Vec<_>>().join(" ");
    if flat.len() > 120 {
        let mut end = 120;
        while !flat.is_char_boundary(end) {
            end -= 1;
        }
        flat.truncate(end);
        flat.push('…');
    }
    flat
}
