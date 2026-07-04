//! The TUI application state and its transition logic.
//!
//! The view is two panes plus a prompt: the tenant's sessions on the left, the
//! selected session's transcript on the right, and an input line below. The
//! committed [`Record`]s the gateway streams are the state's source of truth;
//! the view projects them into styled lines each frame — as a chat transcript,
//! or (with the raw toggle) as the journal JSON the records API actually returns.
//!
//! Network work never runs on the UI task. Every call (list, load, prompt) is
//! spawned, and its results arrive back as [`Update`]s on a channel the main loop
//! drains, so the interface stays responsive while a run is in flight.

use std::sync::Arc;

use tokio::sync::mpsc::UnboundedSender;

use harness::Record;
use harness::RunError;
use harness::RunOutcome;
use harness::Seq;

use crate::client::Event;
use crate::client::GatewayClient;
use crate::client::SessionEntry;

/// Which pane has keyboard focus.
#[derive(Clone, Copy, PartialEq, Eq)]
pub enum Focus {
    Sessions,
    Input,
}

/// What the input line is currently capturing.
#[derive(Clone, Copy, PartialEq, Eq)]
pub enum InputMode {
    /// A prompt for the current session.
    Prompt,
    /// The name of a new session to open.
    NewSession,
}

/// How the transcript pane projects the records.
#[derive(Clone, Copy, PartialEq, Eq)]
pub enum View {
    /// A chat transcript: turns, responses, tool calls, outcomes.
    Chat,
    /// The raw journal: each record as the `/records` API returns it, so the
    /// "the journal IS the session" shape is visible.
    Raw,
}

/// A message from a spawned network task back to the UI loop.
pub enum Update {
    /// The tenant's session list (re)loaded.
    Sessions(Result<Vec<SessionEntry>, String>),
    /// A session's history loaded; replaces the transcript.
    Loaded {
        session: String,
        records: Result<Vec<(Seq, Record)>, String>,
    },
    /// A batch of new records from the live run.
    Records(Vec<(Seq, Record)>),
    /// The run's terminal outcome.
    Outcome(RunOutcome),
    /// The streamed run ended (the SSE stream closed).
    StreamEnded,
    /// A streaming or request error to surface in the status line.
    Error(String),
}

/// The whole application state.
pub struct App {
    client: Arc<GatewayClient>,
    tx: UnboundedSender<Update>,
    pub kind: String,
    pub session: String,
    pub endpoint: String,

    pub sessions: Vec<SessionEntry>,
    pub selected: usize,
    pub focus: Focus,
    pub input: String,
    /// Caret position in `input`, counted in characters from the start.
    pub cursor: usize,
    pub input_mode: InputMode,

    /// The committed records, in sequence order: the source of truth the view
    /// projects each frame, and the watermark a live batch dedups against (the
    /// highest applied seq is just the last record's, see [`App::last_seq`]).
    pub records: Vec<(Seq, Record)>,
    pub streaming: bool,
    /// A per-process nonce and an in-run counter together mint turn ids that are
    /// unique by construction — see [`App::turn_id`].
    nonce: u64,
    turn_counter: u64,

    pub view: View,
    pub show_help: bool,
    pub status: String,
    /// Scroll offset (rows from the top) and whether to stick to the bottom.
    pub scroll: u16,
    pub follow: bool,
    /// The transcript viewport height and wrapped content height, written by the
    /// renderer each frame so the key handlers can page and clamp accurately.
    pub viewport: u16,
    pub content_height: u16,
    pub should_quit: bool,
}

impl App {
    pub fn new(
        client: Arc<GatewayClient>,
        tx: UnboundedSender<Update>,
        kind: String,
        session: String,
        endpoint: String,
        nonce: u64,
    ) -> App {
        App {
            client,
            tx,
            kind,
            session,
            endpoint,
            sessions: Vec::new(),
            selected: 0,
            focus: Focus::Input,
            input: String::new(),
            cursor: 0,
            input_mode: InputMode::Prompt,
            records: Vec::new(),
            streaming: false,
            nonce,
            turn_counter: 0,
            view: View::Chat,
            show_help: false,
            status: "connecting…".to_string(),
            scroll: 0,
            follow: true,
            viewport: 0,
            content_height: 0,
            should_quit: false,
        }
    }

    /// Kick off the initial load: the session list and the starting session's
    /// history.
    pub fn start(&mut self) {
        self.refresh_sessions();
        self.load_session(self.session.clone());
    }

    /// Spawn a refresh of the tenant's session list.
    pub fn refresh_sessions(&self) {
        let client = Arc::clone(&self.client);
        let tx = self.tx.clone();
        let kind = self.kind.clone();
        tokio::spawn(async move {
            let _ = tx.send(Update::Sessions(client.list_sessions(&kind).await));
        });
    }

    /// Switch to `session` and spawn a load of its history.
    pub fn load_session(&mut self, session: String) {
        self.session = session.clone();
        self.records.clear();
        self.follow = true;
        self.scroll = 0;
        self.status = format!("loading {session}…");
        let client = Arc::clone(&self.client);
        let tx = self.tx.clone();
        let kind = self.kind.clone();
        tokio::spawn(async move {
            let records = client.fetch_records(&kind, &session, 0).await;
            let _ = tx.send(Update::Loaded { session, records });
        });
    }

    /// The current turn's id: the client-chosen idempotency key (§7.4). A
    /// per-process `nonce` makes it unique across restarts, so a fresh prompt is
    /// never the persisted id of a finished run — which the grain would dedup,
    /// committing no new records and leaving the stream (and the UI) hung.
    fn turn_id(&self) -> String {
        format!("t-{}-{}", self.nonce, self.turn_counter)
    }

    /// Submit the current input as a turn and start streaming the run.
    pub fn submit_prompt(&mut self) {
        let content = self.input.trim().to_string();
        if content.is_empty() || self.streaming {
            return;
        }
        self.clear_input();
        self.turn_counter += 1;
        let turn = self.turn_id();
        self.streaming = true;
        self.follow = true;
        self.status = format!("running {turn}…");

        let client = Arc::clone(&self.client);
        let tx = self.tx.clone();
        let kind = self.kind.clone();
        let session = self.session.clone();
        let from = self.last_seq();
        tokio::spawn(async move {
            match client
                .open_prompt(&kind, &session, &turn, &content, from)
                .await
            {
                Ok(mut events) => {
                    while let Some(event) = events.next().await {
                        let update = match event {
                            Event::Records(records) => Update::Records(records),
                            Event::Outcome(outcome) => Update::Outcome(outcome),
                            Event::Error(e) => Update::Error(e),
                            Event::End => continue,
                        };
                        if tx.send(update).is_err() {
                            return;
                        }
                    }
                    let _ = tx.send(Update::StreamEnded);
                }
                Err(e) => {
                    let _ = tx.send(Update::Error(e));
                    let _ = tx.send(Update::StreamEnded);
                }
            }
        });
    }

    /// Cancel the in-flight run, if any.
    pub fn cancel(&mut self) {
        if !self.streaming {
            return;
        }
        let turn = self.turn_id();
        self.status = format!("cancelling {turn}…");
        let client = Arc::clone(&self.client);
        let tx = self.tx.clone();
        let kind = self.kind.clone();
        let session = self.session.clone();
        tokio::spawn(async move {
            if let Err(e) = client.cancel(&kind, &session, &turn).await {
                let _ = tx.send(Update::Error(format!("cancel: {e}")));
            }
        });
    }

    /// Begin entering a new session's name on the input line.
    pub fn begin_new_session(&mut self) {
        self.clear_input();
        self.input_mode = InputMode::NewSession;
        self.focus = Focus::Input;
    }

    /// Return the input line to prompt mode, discarding any half-typed name.
    pub fn begin_prompt(&mut self) {
        self.clear_input();
        self.input_mode = InputMode::Prompt;
    }

    /// Open a new session named by the input line.
    pub fn create_session(&mut self) {
        let name = self.input.trim().to_string();
        self.clear_input();
        self.input_mode = InputMode::Prompt;
        self.focus = Focus::Input;
        if name.is_empty() {
            return;
        }
        self.selected = match self.sessions.iter().position(|s| s.session == name) {
            Some(i) => i,
            None => {
                self.sessions.push(SessionEntry {
                    session: name.clone(),
                    label: None,
                });
                self.sessions.len() - 1
            }
        };
        self.load_session(name);
    }

    /// Move the session-list selection by `delta` and load the selection.
    pub fn select_session(&mut self, delta: isize) {
        if self.sessions.is_empty() {
            return;
        }
        let len = self.sessions.len() as isize;
        let next = (self.selected as isize + delta).rem_euclid(len) as usize;
        self.selected = next;
        let session = self.sessions[next].session.clone();
        self.load_session(session);
    }

    /// Toggle between the chat and raw-journal projections of the records.
    pub fn toggle_view(&mut self) {
        self.view = match self.view {
            View::Chat => View::Raw,
            View::Raw => View::Chat,
        };
    }

    /// The largest in-bounds scroll offset for the last rendered frame.
    pub fn max_scroll(&self) -> u16 {
        self.content_height.saturating_sub(self.viewport)
    }

    /// Scroll the transcript by `delta` rows, clamped to the content. Reaching
    /// the bottom re-attaches the follow-the-tail view; leaving it detaches.
    pub fn scroll(&mut self, delta: isize) {
        let max = self.max_scroll();
        let next = (self.scroll as isize + delta).clamp(0, max as isize) as u16;
        self.scroll = next;
        self.follow = next >= max;
    }

    /// Scroll by whole pages (one viewport height, less a row of overlap).
    pub fn scroll_page(&mut self, pages: isize) {
        let page = (self.viewport.saturating_sub(1)).max(1) as isize;
        self.scroll(pages * page);
    }

    /// Jump to the top, detaching the follow view.
    pub fn scroll_to_top(&mut self) {
        self.scroll = 0;
        self.follow = false;
    }

    /// Jump to the bottom and re-attach the follow view.
    pub fn scroll_to_bottom(&mut self) {
        self.scroll = self.max_scroll();
        self.follow = true;
    }

    /// Insert a character at the caret.
    pub fn input_insert(&mut self, c: char) {
        let at = self.byte_at(self.cursor);
        self.input.insert(at, c);
        self.cursor += 1;
    }

    /// Delete the character before the caret.
    pub fn input_backspace(&mut self) {
        if self.cursor == 0 {
            return;
        }
        let prev = self.cursor - 1;
        let at = self.byte_at(prev);
        self.input.remove(at);
        self.cursor = prev;
    }

    /// Move the caret by `delta` characters, clamped to the line.
    pub fn move_cursor(&mut self, delta: isize) {
        let len = self.input.chars().count() as isize;
        self.cursor = (self.cursor as isize + delta).clamp(0, len) as usize;
    }

    /// Move the caret to the start or end of the line.
    pub fn cursor_home(&mut self) {
        self.cursor = 0;
    }
    pub fn cursor_end(&mut self) {
        self.cursor = self.input.chars().count();
    }

    /// Empty the input line and reset the caret.
    pub fn clear_input(&mut self) {
        self.input.clear();
        self.cursor = 0;
    }

    /// Handle Esc on the prompt line: abandon a half-typed session name, cancel
    /// a running turn, or clear the line.
    pub fn escape(&mut self) {
        if self.input_mode == InputMode::NewSession {
            self.begin_prompt();
        } else if self.streaming {
            self.cancel();
        } else {
            self.clear_input();
        }
    }

    /// The byte offset of the caret, for splitting `input` at the cursor.
    pub fn cursor_byte(&self) -> usize {
        self.byte_at(self.cursor)
    }

    /// The byte offset of the `n`th character (or the end of the string).
    fn byte_at(&self, n: usize) -> usize {
        self.input
            .char_indices()
            .nth(n)
            .map(|(b, _)| b)
            .unwrap_or(self.input.len())
    }

    /// Apply one update from a network task.
    pub fn apply(&mut self, update: Update) {
        match update {
            Update::Sessions(Ok(sessions)) => {
                self.sessions = sessions;
                // Keep the current session present even before its first prompt
                // records it in the directory, so it never vanishes mid-typing.
                if !self.sessions.iter().any(|s| s.session == self.session) {
                    self.sessions.insert(
                        0,
                        SessionEntry {
                            session: self.session.clone(),
                            label: None,
                        },
                    );
                }
                self.selected = self
                    .sessions
                    .iter()
                    .position(|s| s.session == self.session)
                    .unwrap_or(0);
                if !self.streaming {
                    self.status = self.idle_status();
                }
            }
            Update::Sessions(Err(e)) => self.status = format!("sessions: {e}"),
            Update::Loaded { session, records } => {
                if session != self.session {
                    return; // a stale load for a session we've since left.
                }
                match records {
                    Ok(records) => {
                        self.records = records;
                        self.status = self.idle_status();
                    }
                    Err(e) => self.status = format!("load {session}: {e}"),
                }
            }
            Update::Records(records) => {
                for (seq, record) in records {
                    self.push_record(seq, record);
                }
            }
            Update::Outcome(outcome) => {
                self.status = match &outcome {
                    Ok(c) => format!("done — {} tokens", c.tokens()),
                    Err(RunError::BudgetExhausted) => "budget exhausted".to_string(),
                    Err(RunError::Cancelled) => "cancelled".to_string(),
                    Err(RunError::Model(e)) => format!("model error: {e}"),
                };
            }
            Update::StreamEnded => {
                self.streaming = false;
                if self.status.starts_with("running") || self.status.starts_with("cancelling") {
                    self.status = self.idle_status();
                }
                self.refresh_sessions();
            }
            Update::Error(e) => self.status = format!("error: {e}"),
        }
    }

    fn idle_status(&self) -> String {
        format!(
            "{} session{}",
            self.sessions.len(),
            plural(self.sessions.len())
        )
    }

    /// The highest record sequence applied — just the last record's, since records
    /// are appended in order. The watermark a live batch dedups against.
    fn last_seq(&self) -> u64 {
        self.records.last().map(|(s, _)| s.value()).unwrap_or(0)
    }

    /// Append a record, skipping anything already applied.
    fn push_record(&mut self, seq: Seq, record: Record) {
        if seq.value() <= self.last_seq() {
            return;
        }
        self.records.push((seq, record));
    }
}

fn plural(n: usize) -> &'static str {
    if n == 1 { "" } else { "s" }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn app(nonce: u64) -> App {
        let (tx, _rx) = tokio::sync::mpsc::unbounded_channel();
        let client = GatewayClient::new("http://127.0.0.1:8080", "alice").unwrap();
        App::new(
            client,
            tx,
            "assistant".to_string(),
            "demo".to_string(),
            "http://127.0.0.1:8080".to_string(),
            nonce,
        )
    }

    #[test]
    fn turn_ids_advance_within_a_run() {
        let mut a = app(42);
        a.turn_counter += 1;
        assert_eq!(a.turn_id(), "t-42-1");
        a.turn_counter += 1;
        assert_eq!(a.turn_id(), "t-42-2");
    }

    #[test]
    fn a_restart_cannot_reuse_a_prior_runs_turn_id() {
        // Same counter, different process nonce — the ids never collide, so a
        // fresh prompt after a restart is never dedup'd against the journal.
        let (mut first, mut second) = (app(1), app(2));
        first.turn_counter += 1;
        second.turn_counter += 1;
        assert_ne!(first.turn_id(), second.turn_id());
    }
}
