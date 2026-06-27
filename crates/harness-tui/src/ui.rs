//! Rendering: the two-pane-plus-prompt layout drawn from [`App`] each frame.
//!
//! Layout is the sessions list (left), the transcript (right), the input line,
//! and a status/hints bar. The transcript is word-wrapped *here* into physical
//! rows so the scroll offset is exact — when "following", the view parks at the
//! bottom; scrolling up detaches until the user pages back down. A `?` overlay
//! maps every key to the gateway endpoint it drives.

use ratatui::Frame;
use ratatui::layout::Constraint;
use ratatui::layout::Direction;
use ratatui::layout::Layout;
use ratatui::layout::Rect;
use ratatui::style::Color;
use ratatui::style::Modifier;
use ratatui::style::Style;
use ratatui::text::Line;
use ratatui::text::Span;
use ratatui::widgets::Block;
use ratatui::widgets::Borders;
use ratatui::widgets::Clear;
use ratatui::widgets::List;
use ratatui::widgets::ListItem;
use ratatui::widgets::ListState;
use ratatui::widgets::Paragraph;

use harness::Record;
use harness::RecordBody;
use harness::Seq;

use crate::app::App;
use crate::app::Focus;
use crate::app::InputMode;
use crate::app::View;

const ACCENT: Color = Color::Cyan;
const DIM: Color = Color::DarkGray;

const USER: Color = Color::Cyan;
const ASSISTANT: Color = Color::White;
const TOOL: Color = Color::Yellow;
const OK: Color = Color::Green;
const ERR: Color = Color::Red;
const DELEGATE: Color = Color::Magenta;

/// Tallest the input box grows before it scrolls internally.
const INPUT_MAX_ROWS: u16 = 6;

/// Width of the sessions sidebar (including its borders).
const SIDEBAR_WIDTH: u16 = 26;

/// Draw the whole interface for one frame.
///
/// The sessions list is a full-height sidebar on the left; the conversation and
/// its prompt stack in the column to its right, above a full-width status bar.
pub fn render(frame: &mut Frame, app: &mut App) {
    let area = frame.area();

    let outer = Layout::default()
        .direction(Direction::Vertical)
        .constraints([
            Constraint::Min(3),    // body (sidebar + conversation)
            Constraint::Length(1), // status
        ])
        .split(area);

    let body = Layout::default()
        .direction(Direction::Horizontal)
        .constraints([Constraint::Length(SIDEBAR_WIDTH), Constraint::Min(20)])
        .split(outer[0]);

    // The prompt box grows with its content up to a cap, so a multi-line prompt
    // is visible while typing. It is as wide as the conversation column now.
    let input_rows = wrap_count(&app.input, body[1].width.saturating_sub(2) as usize)
        .clamp(1, INPUT_MAX_ROWS as usize) as u16;
    let right = Layout::default()
        .direction(Direction::Vertical)
        .constraints([
            Constraint::Min(3),                 // transcript
            Constraint::Length(input_rows + 2), // prompt (+borders)
        ])
        .split(body[1]);

    render_sessions(frame, app, body[0]);
    render_transcript(frame, app, right[0]);
    render_input(frame, app, right[1]);
    render_status(frame, app, outer[1]);

    if app.show_help {
        render_help(frame, area);
    }
}

fn render_sessions(frame: &mut Frame, app: &App, area: Rect) {
    let focused = app.focus == Focus::Sessions;
    let items: Vec<ListItem> = app
        .sessions
        .iter()
        .map(|s| {
            let label = match &s.label {
                Some(l) if l != &s.session => format!("{} ({l})", s.session),
                _ => s.session.clone(),
            };
            ListItem::new(label)
        })
        .collect();
    let mut state = ListState::default();
    if !app.sessions.is_empty() {
        state.select(Some(app.selected.min(app.sessions.len() - 1)));
    }
    let title = format!("sessions ({})", app.sessions.len());
    let list = List::new(items)
        .block(block(&title, focused))
        .highlight_style(
            Style::default()
                .fg(ACCENT)
                .add_modifier(Modifier::BOLD | Modifier::REVERSED),
        )
        .highlight_symbol("▌");
    frame.render_stateful_widget(list, area, &mut state);
}

fn render_transcript(frame: &mut Frame, app: &mut App, area: Rect) {
    let inner_width = area.width.saturating_sub(2) as usize;
    let viewport = area.height.saturating_sub(2) as usize;

    // Project the records to logical lines, then word-wrap into physical rows so
    // the row count — and therefore the scroll math — is exact rather than
    // estimated. Rendering the pre-wrapped slice means no widget re-wrapping can
    // disagree with our offset and clip the final record.
    let logical = if app.records.is_empty() {
        vec![Line::styled(
            "  no records yet — type a prompt below and press Enter",
            Style::default().fg(DIM),
        )]
    } else {
        match app.view {
            View::Chat => app
                .records
                .iter()
                .flat_map(|(_, r)| record_lines(r))
                .collect(),
            View::Raw => app
                .records
                .iter()
                .flat_map(|(seq, r)| raw_lines(*seq, r))
                .collect(),
        }
    };
    let wrapped: Vec<Line> = logical
        .iter()
        .flat_map(|line| wrap_line(line, inner_width))
        .collect();

    app.content_height = wrapped.len() as u16;
    app.viewport = viewport as u16;
    let max_scroll = app.max_scroll();
    app.scroll = if app.follow {
        max_scroll
    } else {
        app.scroll.min(max_scroll)
    };

    let start = app.scroll as usize;
    let visible: Vec<Line> = wrapped.into_iter().skip(start).take(viewport).collect();

    let title = transcript_title(app, max_scroll);
    let paragraph = Paragraph::new(visible).block(block(&title, app.focus == Focus::Input));
    frame.render_widget(paragraph, area);
}

/// The transcript title: address, view mode, and the scroll position so it is
/// obvious whether the tail is being followed or history is being browsed.
fn transcript_title(app: &App, max_scroll: u16) -> String {
    let mode = match app.view {
        View::Chat => "chat",
        View::Raw => "raw",
    };
    let position = if app.follow || max_scroll == 0 {
        "▼ live".to_string()
    } else {
        format!("↑ {}/{}", app.scroll, max_scroll)
    };
    format!("{}/{} · {mode} · {position}", app.kind, app.session)
}

fn render_input(frame: &mut Frame, app: &App, area: Rect) {
    let (title, accent) = match app.input_mode {
        InputMode::Prompt => ("prompt", ACCENT),
        InputMode::NewSession => ("new session name", Color::Magenta),
    };
    let focused = app.focus == Focus::Input;
    // Split the line at the caret so the cursor shows where edits land.
    let (left, right) = app.input.split_at(app.cursor_byte());
    let mut spans = vec![Span::raw(left)];
    if focused {
        spans.push(Span::styled("▏", Style::default().fg(accent)));
    }
    spans.push(Span::raw(right));
    let input = Paragraph::new(Line::from(spans)).block(block(title, focused));
    frame.render_widget(input, area);
}

fn render_status(frame: &mut Frame, app: &App, area: Rect) {
    let hints = match app.focus {
        Focus::Input => "Tab sessions · Enter send · Esc cancel · ^N new · ^R raw · ? help",
        Focus::Sessions => "Tab input · ↑/↓ select · n new · PgUp/PgDn scroll · ? help",
    };
    let spinner = if app.streaming { "● " } else { "" };
    let line = Line::from(vec![
        Span::styled(
            format!(" {spinner}{}", app.status),
            Style::default().fg(ACCENT),
        ),
        Span::raw("  "),
        Span::styled(app.endpoint.as_str(), Style::default().fg(DIM)),
        Span::raw("  "),
        Span::styled(hints, Style::default().fg(DIM)),
    ]);
    frame.render_widget(Paragraph::new(line), area);
}

/// A centered overlay mapping each key to the gateway endpoint it drives, so the
/// REST/SSE surface is discoverable from inside the client.
fn render_help(frame: &mut Frame, area: Rect) {
    let rows: &[(&str, &str)] = &[
        (
            "Enter (prompt)",
            "POST /v1/{kind}/{session}/prompt  (SSE stream)",
        ),
        ("Esc (running)", "POST /v1/{kind}/{session}/cancel"),
        (
            "↑/↓ in sessions",
            "GET  /v1/{kind}/{session}/records  (load history)",
        ),
        (
            "startup / on end",
            "GET  /v1/sessions?kind={kind}  (this tenant)",
        ),
        ("n · Ctrl-N", "new session (recorded on its first prompt)"),
        ("Ctrl-R", "toggle raw journal view (the /records payload)"),
        ("", ""),
        ("Tab", "switch focus (sessions ⇄ prompt)"),
        ("PgUp/PgDn", "scroll a page · Home/End top/bottom"),
        ("mouse wheel", "scroll the transcript"),
        ("← → Home End", "move the caret in the prompt"),
        ("? / F1", "toggle this help · Ctrl-C quit"),
    ];

    let mut lines = vec![
        Line::styled(
            " harness-tui — keys map to gateway endpoints",
            Style::default().fg(ACCENT).add_modifier(Modifier::BOLD),
        ),
        Line::raw(""),
    ];
    for (key, action) in rows {
        if key.is_empty() {
            lines.push(Line::raw(""));
            continue;
        }
        lines.push(Line::from(vec![
            Span::styled(
                format!(" {key:<16}"),
                Style::default().fg(ACCENT).add_modifier(Modifier::BOLD),
            ),
            Span::styled(action.to_string(), Style::default().fg(Color::Gray)),
        ]));
    }
    lines.push(Line::raw(""));
    lines.push(Line::styled(
        " press any key to dismiss",
        Style::default().fg(DIM).add_modifier(Modifier::ITALIC),
    ));

    let width = 66.min(area.width.saturating_sub(4));
    let height = (lines.len() as u16 + 2).min(area.height.saturating_sub(2));
    let popup = centered(area, width, height);
    frame.render_widget(Clear, popup);
    let block = Block::default()
        .borders(Borders::ALL)
        .border_style(Style::default().fg(ACCENT))
        .title(Span::styled(
            " help ",
            Style::default().fg(ACCENT).add_modifier(Modifier::BOLD),
        ));
    frame.render_widget(Paragraph::new(lines).block(block), popup);
}

/// A `width`×`height` rectangle centered in `area`.
fn centered(area: Rect, width: u16, height: u16) -> Rect {
    let x = area.x + (area.width.saturating_sub(width)) / 2;
    let y = area.y + (area.height.saturating_sub(height)) / 2;
    Rect {
        x,
        y,
        width,
        height,
    }
}

/// A pane border, brightened when the pane holds focus.
fn block(title: &str, focused: bool) -> Block<'static> {
    let color = if focused { ACCENT } else { DIM };
    Block::default()
        .borders(Borders::ALL)
        .border_style(Style::default().fg(color))
        .title(Span::styled(
            format!(" {title} "),
            Style::default()
                .fg(if focused { ACCENT } else { Color::Gray })
                .add_modifier(Modifier::BOLD),
        ))
}

// ---------------------------------------------------------------------------
// Record projections
// ---------------------------------------------------------------------------

/// Project one record onto styled chat lines.
fn record_lines(record: &Record) -> Vec<Line<'static>> {
    match &record.body {
        RecordBody::SessionCreated { kind, .. } => {
            vec![dim(format!("— session created · kind {kind}"))]
        }
        RecordBody::TurnSubmitted { content, .. } => {
            let mut lines = vec![Line::raw("")];
            lines.extend(labelled("you", content, USER, true));
            lines
        }
        RecordBody::ModelResponse {
            content,
            calls,
            usage,
            ..
        } => {
            let mut lines = Vec::new();
            if !content.trim().is_empty() {
                lines.extend(labelled("assistant", content, ASSISTANT, false));
            }
            for call in calls {
                let input = compact(&call.input, 100);
                lines.push(Line::from(vec![
                    Span::styled("  ⚙ ", Style::default().fg(TOOL)),
                    Span::styled(
                        call.name.clone(),
                        Style::default().fg(TOOL).add_modifier(Modifier::BOLD),
                    ),
                    Span::raw(" "),
                    Span::styled(input, Style::default().fg(DIM)),
                ]));
            }
            let total = usage.total();
            if total > 0 {
                lines.push(dim(format!(
                    "    · {} in / {} out tokens",
                    usage.input_tokens, usage.output_tokens
                )));
            }
            lines
        }
        RecordBody::ToolOutcome { outcome, .. } => match outcome {
            Ok(value) => vec![Line::from(vec![
                Span::styled("    ✓ ", Style::default().fg(OK)),
                Span::styled(compact(value, 160), Style::default().fg(DIM)),
            ])],
            Err(e) => vec![Line::from(vec![
                Span::styled("    ✗ ", Style::default().fg(ERR)),
                Span::styled(e.to_string(), Style::default().fg(ERR)),
            ])],
        },
        RecordBody::ChildRun {
            child_kind,
            child_session,
            ..
        } => vec![Line::from(vec![
            Span::styled("  ↳ delegate → ", Style::default().fg(DELEGATE)),
            Span::styled(
                format!("{child_kind}/{child_session}"),
                Style::default().fg(DELEGATE),
            ),
        ])],
        RecordBody::WorkspaceReset => vec![dim("— workspace reset".to_string())],
        RecordBody::TierAcquired { tier, .. } => {
            vec![dim(format!("— tier acquired · {tier:?}"))]
        }
        RecordBody::RunEnded { turn, outcome } => {
            let (mark, color) = match outcome {
                Ok(_) => ("ok", OK),
                Err(_) => ("error", ERR),
            };
            let detail = match outcome {
                Ok(c) => format!("— turn {turn} {mark} · {} tokens", c.tokens),
                Err(e) => format!("— turn {turn} {mark} · {e}"),
            };
            vec![Line::styled(detail, Style::default().fg(color))]
        }
    }
}

/// Project one record as the raw journal entry the `/records` API returns: a
/// dimmed `seq` header followed by its pretty-printed JSON.
fn raw_lines(seq: Seq, record: &Record) -> Vec<Line<'static>> {
    let mut lines = vec![Line::styled(
        format!("{seq} ─"),
        Style::default().fg(ACCENT).add_modifier(Modifier::BOLD),
    )];
    let json = serde_json::to_string_pretty(record)
        .unwrap_or_else(|e| format!("<unserializable record: {e}>"));
    for raw in json.lines() {
        lines.push(Line::styled(
            format!("  {raw}"),
            Style::default().fg(Color::Gray),
        ));
    }
    lines.push(Line::raw(""));
    lines
}

/// A labelled, possibly multi-line block: the first line carries the `who ▸`
/// prefix, continuations align under it.
fn labelled(who: &str, content: &str, color: Color, bold: bool) -> Vec<Line<'static>> {
    let mut style = Style::default().fg(color);
    if bold {
        style = style.add_modifier(Modifier::BOLD);
    }
    let label = format!("{who} ▸ ");
    let pad = " ".repeat(label.chars().count());
    let mut lines = Vec::new();
    for (i, raw) in content.lines().enumerate() {
        let prefix = if i == 0 { label.clone() } else { pad.clone() };
        lines.push(Line::from(vec![
            Span::styled(prefix, style),
            Span::styled(raw.to_string(), Style::default().fg(color)),
        ]));
    }
    if lines.is_empty() {
        lines.push(Line::styled(label, style));
    }
    lines
}

fn dim(text: String) -> Line<'static> {
    Line::styled(
        text,
        Style::default().fg(DIM).add_modifier(Modifier::ITALIC),
    )
}

/// A compact, length-capped rendering of a JSON value for one transcript line.
fn compact(value: &serde_json::Value, max: usize) -> String {
    let text = match value {
        serde_json::Value::String(s) => s.clone(),
        other => other.to_string(),
    };
    let text: String = text.split_whitespace().collect::<Vec<_>>().join(" ");
    if text.chars().count() > max {
        let kept: String = text.chars().take(max).collect();
        format!("{kept}…")
    } else {
        text
    }
}

// ---------------------------------------------------------------------------
// Word wrapping
// ---------------------------------------------------------------------------

/// Word-wrap one logical line into physical rows at `width`, preserving each
/// span's style. Wrapping here (instead of via a widget's `Wrap`) is what makes
/// the scroll offset exact: the rendered row count is the count we scroll over.
fn wrap_line(line: &Line<'static>, width: usize) -> Vec<Line<'static>> {
    if width == 0 {
        return vec![line.clone()];
    }
    let atoms: Vec<(char, Style)> = line
        .spans
        .iter()
        .flat_map(|span| span.content.chars().map(move |c| (c, span.style)))
        .collect();
    if atoms.is_empty() {
        return vec![Line::default()];
    }

    let mut rows: Vec<Vec<(char, Style)>> = Vec::new();
    let mut cur: Vec<(char, Style)> = Vec::new();
    let mut word: Vec<(char, Style)> = Vec::new();

    for (ch, style) in atoms {
        if ch == ' ' || ch == '\t' {
            flush_word(&mut rows, &mut cur, &mut word, width);
            if cur.len() < width {
                cur.push((' ', style));
            } else {
                // A space at the wrap boundary just ends the row; the trailing
                // run of spaces is dropped rather than starting the next row.
                rows.push(std::mem::take(&mut cur));
            }
        } else {
            word.push((ch, style));
        }
    }
    flush_word(&mut rows, &mut cur, &mut word, width);
    if !cur.is_empty() || rows.is_empty() {
        rows.push(cur);
    }

    rows.into_iter().map(to_line).collect()
}

/// Place the pending word onto the current row, wrapping first if it would not
/// fit; a word longer than the whole width is hard-split across rows.
fn flush_word(
    rows: &mut Vec<Vec<(char, Style)>>,
    cur: &mut Vec<(char, Style)>,
    word: &mut Vec<(char, Style)>,
    width: usize,
) {
    if word.is_empty() {
        return;
    }
    if word.len() > width {
        for atom in word.drain(..) {
            if cur.len() >= width {
                rows.push(std::mem::take(cur));
            }
            cur.push(atom);
        }
    } else {
        if cur.len() + word.len() > width {
            rows.push(std::mem::take(cur));
        }
        cur.append(word);
    }
    word.clear();
}

/// Regroup a row of styled characters into a `Line`, coalescing runs that share
/// a style into one span.
fn to_line(row: Vec<(char, Style)>) -> Line<'static> {
    let mut spans: Vec<Span<'static>> = Vec::new();
    let mut buf = String::new();
    let mut style: Option<Style> = None;
    for (ch, st) in row {
        if style != Some(st) {
            if let Some(prev) = style {
                spans.push(Span::styled(std::mem::take(&mut buf), prev));
            }
            style = Some(st);
        }
        buf.push(ch);
    }
    if let Some(prev) = style {
        spans.push(Span::styled(buf, prev));
    }
    Line::from(spans)
}

/// How many rows `text` wraps to at `width` (used to size the input box).
fn wrap_count(text: &str, width: usize) -> usize {
    if width == 0 {
        return 1;
    }
    let line = Line::raw(text.to_string());
    wrap_line(&line, width).len()
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The flattened text of a wrapped row, dropping styling.
    fn text(line: &Line) -> String {
        line.spans.iter().map(|s| s.content.as_ref()).collect()
    }

    #[test]
    fn wraps_on_word_boundaries_without_splitting_words() {
        let line = Line::raw("the quick brown fox jumps");
        let rows = wrap_line(&line, 10);
        // "the quick " is 10, "brown fox " is 10, then "jumps".
        let texts: Vec<String> = rows.iter().map(text).collect();
        assert_eq!(texts, vec!["the quick ", "brown fox ", "jumps"]);
        // No row exceeds the width — the property the scroll math depends on.
        for row in &rows {
            assert!(text(row).chars().count() <= 10);
        }
    }

    #[test]
    fn hard_splits_a_word_longer_than_the_width() {
        let line = Line::raw("supercalifragilistic");
        let rows = wrap_line(&line, 8);
        assert_eq!(rows.len(), 3);
        for row in &rows {
            assert!(text(row).chars().count() <= 8);
        }
        let joined: String = rows.iter().map(text).collect();
        assert_eq!(joined, "supercalifragilistic");
    }

    #[test]
    fn an_empty_line_is_one_row() {
        assert_eq!(wrap_line(&Line::raw(""), 10).len(), 1);
        assert_eq!(wrap_count("", 10), 1);
    }

    #[test]
    fn wrapping_preserves_per_span_styles() {
        // A styled prefix followed by plain content, as the chat lines use.
        let line = Line::from(vec![
            Span::styled("you ▸ ", Style::default().fg(USER)),
            Span::raw("hello there friend"),
        ]);
        let rows = wrap_line(&line, 9);
        // The prefix keeps its color on the first row.
        let first = &rows[0];
        assert_eq!(first.spans[0].style.fg, Some(USER));
        let joined: String = rows.iter().map(text).collect();
        assert!(joined.starts_with("you ▸ "));
        assert!(joined.contains("friend"));
    }

    #[test]
    fn raw_view_emits_a_seq_header_and_json_body() {
        use harness::Record;
        use harness::RecordBody;

        let record = Record {
            at_nanos: 0,
            body: RecordBody::WorkspaceReset,
        };
        let lines = raw_lines(Seq::new(7), &record);
        assert!(text(&lines[0]).starts_with("seq-7"));
        let body: String = lines.iter().map(|l| text(l)).collect();
        assert!(body.contains("WorkspaceReset"));
    }
}
