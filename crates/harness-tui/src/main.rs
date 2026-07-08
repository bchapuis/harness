//! `harness-tui`: a terminal client for the agentic harness.
//!
//! It talks to a `harness-gateway` over its HTTP/SSE surface — nothing else. It
//! authenticates as a tenant with a bearer token, lists that tenant's sessions,
//! and drives one: submit a prompt and watch the run's records stream in as a
//! chat transcript. The gateway does the cluster work; this is just the edge UI.
//!
//! ```text
//! harness-tui [--url http://127.0.0.1:8080] [--token anonymous]
//!             [--kind assistant] [--session demo]
//! ```
//!
//! Defaults match the loopback demo (`demo.sh`): the insecure gateway takes the
//! bearer token as the tenant, so the default `--token anonymous` simply acts as
//! tenant "anonymous". Against an authenticated gateway, pass the opaque API
//! token instead.

mod app;
mod client;
mod ui;

use crossterm::event::DisableMouseCapture;
use crossterm::event::EnableMouseCapture;
use crossterm::event::Event as TermEvent;
use crossterm::event::EventStream;
use crossterm::event::KeyCode;
use crossterm::event::KeyEvent;
use crossterm::event::KeyEventKind;
use crossterm::event::KeyModifiers;
use crossterm::event::MouseEvent;
use crossterm::event::MouseEventKind;
use crossterm::execute;
use futures::StreamExt;
use tokio::sync::mpsc;

use crate::app::App;
use crate::app::Focus;
use crate::app::InputMode;
use crate::client::GatewayClient;

const USAGE: &str = "\
usage:
  harness-tui [options]

options (defaults in parentheses):
  --url <base>       gateway base url, http(s)://host[:port]  (http://127.0.0.1:8080)
  --token <t>        tenant bearer token; $HARNESS_TOKEN, else (anonymous)
  --kind <k>         agent kind to address                    (assistant)
  --session <s>      session to open on start                 (demo)

Keys: Tab switches panes · type a prompt and press Enter · Esc cancels a run ·
Ctrl-N opens a new session · Ctrl-R toggles the raw journal view · PgUp/PgDn and
the mouse wheel scroll · ? shows the full key/endpoint map · Ctrl-C quits.";

#[tokio::main]
async fn main() {
    let args: Vec<String> = std::env::args().skip(1).collect();
    if matches!(args.first().map(String::as_str), Some("--help" | "-h")) {
        println!("{USAGE}");
        return;
    }
    if let Err(message) = run(args).await {
        eprintln!("error: {message}\n\n{USAGE}");
        std::process::exit(1);
    }
}

async fn run(args: Vec<String>) -> Result<(), String> {
    let mut url = "http://127.0.0.1:8080".to_string();
    let mut token = std::env::var("HARNESS_TOKEN").unwrap_or_else(|_| "anonymous".to_string());
    let mut kind = "assistant".to_string();
    let mut session = "demo".to_string();

    let mut i = 0;
    while i < args.len() {
        let flag = args[i].as_str();
        let value = args
            .get(i + 1)
            .ok_or_else(|| format!("{flag} requires a value"))?;
        match flag {
            "--url" => url = value.clone(),
            "--token" => token = value.clone(),
            "--kind" => kind = value.clone(),
            "--session" => session = value.clone(),
            other => return Err(format!("unknown flag: {other}")),
        }
        i += 2;
    }

    let client = GatewayClient::new(&url, token)?;

    // A per-process nonce so freshly minted turn ids never collide with a prior
    // run's persisted ids after a restart (which the grain would dedup, hanging
    // the run's stream). The pid changes across restarts and needs no wall clock
    // (§18.1 forbids reading it here).
    let nonce = u64::from(std::process::id());

    let (tx, mut rx) = mpsc::unbounded_channel();
    let mut app = App::new(client, tx, kind, session, url, nonce);
    app.start();

    let mut terminal = ratatui::init();
    // Capture the mouse so the wheel can scroll the transcript. Best-effort: a
    // terminal that refuses still works through the keyboard.
    let _ = execute!(std::io::stdout(), EnableMouseCapture);
    let mut events = EventStream::new();
    let result = loop {
        if let Err(e) = terminal.draw(|frame| ui::render(frame, &mut app)) {
            break Err(format!("draw: {e}"));
        }
        if app.should_quit {
            break Ok(());
        }
        tokio::select! {
            term = events.next() => match term {
                Some(Ok(TermEvent::Key(key))) if key.kind == KeyEventKind::Press => {
                    handle_key(&mut app, key);
                }
                Some(Ok(TermEvent::Mouse(mouse))) => handle_mouse(&mut app, mouse),
                Some(Ok(_)) => {}
                Some(Err(e)) => break Err(format!("input: {e}")),
                None => break Ok(()),
            },
            update = rx.recv() => match update {
                Some(update) => app.apply(update),
                None => break Ok(()),
            },
        }
    };
    let _ = execute!(std::io::stdout(), DisableMouseCapture);
    ratatui::restore();
    result.map_err(|e: String| e)
}

/// Scroll the transcript on a mouse wheel turn (three rows per notch).
fn handle_mouse(app: &mut App, mouse: MouseEvent) {
    match mouse.kind {
        MouseEventKind::ScrollUp => app.scroll(-3),
        MouseEventKind::ScrollDown => app.scroll(3),
        _ => {}
    }
}

/// Map one key press onto an action. Global keys (quit, help, focus, scrolling,
/// view/session toggles) are handled first; the rest depends on the focused pane.
fn handle_key(app: &mut App, key: KeyEvent) {
    // While the help overlay is up, any key dismisses it (Ctrl-C still quits).
    if app.show_help {
        if is_quit(key) {
            app.should_quit = true;
        } else {
            app.show_help = false;
        }
        return;
    }
    if is_quit(key) {
        app.should_quit = true;
        return;
    }
    let ctrl = key.modifiers.contains(KeyModifiers::CONTROL);
    match key.code {
        KeyCode::Char('r') if ctrl => {
            app.toggle_view();
            return;
        }
        KeyCode::Char('n') if ctrl => {
            app.begin_new_session();
            return;
        }
        KeyCode::F(1) => {
            app.show_help = true;
            return;
        }
        KeyCode::Tab | KeyCode::BackTab => {
            app.focus = match app.focus {
                Focus::Input => Focus::Sessions,
                Focus::Sessions => Focus::Input,
            };
            return;
        }
        KeyCode::PageUp => {
            app.scroll_page(-1);
            return;
        }
        KeyCode::PageDown => {
            app.scroll_page(1);
            return;
        }
        _ => {}
    }
    match app.focus {
        Focus::Sessions => handle_sessions_key(app, key),
        Focus::Input => handle_input_key(app, key),
    }
}

fn is_quit(key: KeyEvent) -> bool {
    key.modifiers.contains(KeyModifiers::CONTROL) && matches!(key.code, KeyCode::Char('c'))
}

fn handle_sessions_key(app: &mut App, key: KeyEvent) {
    match key.code {
        KeyCode::Up => app.select_session(-1),
        KeyCode::Down => app.select_session(1),
        KeyCode::Home => app.scroll_to_top(),
        KeyCode::End => app.scroll_to_bottom(),
        KeyCode::Char('n') => app.begin_new_session(),
        KeyCode::Char('?') => app.show_help = true,
        KeyCode::Enter => app.focus = Focus::Input,
        _ => {}
    }
}

fn handle_input_key(app: &mut App, key: KeyEvent) {
    match key.code {
        KeyCode::Enter => match app.input_mode {
            InputMode::Prompt => app.submit_prompt(),
            InputMode::NewSession => app.create_session(),
        },
        KeyCode::Esc => app.escape(),
        KeyCode::Backspace => app.input_backspace(),
        KeyCode::Left => app.move_cursor(-1),
        KeyCode::Right => app.move_cursor(1),
        KeyCode::Home => app.cursor_home(),
        KeyCode::End => app.cursor_end(),
        // Up/Down scroll the transcript: the prompt pane has no use for them.
        KeyCode::Up => app.scroll(-1),
        KeyCode::Down => app.scroll(1),
        KeyCode::Char(c) => app.input_insert(c),
        _ => {}
    }
}
