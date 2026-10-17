//! The markdown the specifications are written in, read one way for every gate.
//!
//! Each gate needs a little of it: where a section starts and how deep its heading
//! sits, which lines a code fence holds, what number a list item carries, what a
//! `§` names, and an excerpt to quote when something fails. Two gates reading a
//! heading differently would disagree about where a section ends, so the reading
//! lives here once.

/// Where a line sits relative to fenced code blocks.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Fence<'a> {
    /// Prose.
    Outside,
    /// The fence line opening a block, with its info string (`rust`, `sh`, or
    /// empty).
    Opens(&'a str),
    /// A line inside a block.
    Inside,
    /// The fence line closing a block.
    Closes,
}

/// One line of a document, with where it sits.
#[derive(Debug, Clone, Copy)]
pub(crate) struct Line<'a> {
    /// Zero-based line number.
    pub index: usize,
    /// Byte offset of the line's first character.
    pub start: usize,
    /// Byte offset just past the line's terminator: where the next line starts.
    pub end: usize,
    /// The line without its terminator, as [`str::lines`] yields it.
    pub text: &'a str,
    /// Where it sits relative to fenced code.
    pub fence: Fence<'a>,
}

impl Line<'_> {
    /// Whether the line belongs to a code block. A fence line itself does: a
    /// gate skipping fenced content skips the fences with it.
    pub fn fenced(&self) -> bool {
        self.fence != Fence::Outside
    }
}

/// Every line of `text` in order. Any line opening with three backticks toggles
/// a fence, whatever its info string.
pub(crate) fn lines(text: &str) -> impl Iterator<Item = Line<'_>> {
    let mut inside = false;
    let mut start = 0usize;
    text.split_inclusive('\n')
        .enumerate()
        .map(move |(index, raw)| {
            let end = start + raw.len();
            let line = raw.strip_suffix('\n').unwrap_or(raw);
            let line = line.strip_suffix('\r').unwrap_or(line);
            let trimmed = line.trim_start();
            let fence = if trimmed.starts_with("```") {
                inside = !inside;
                if inside {
                    Fence::Opens(trimmed.trim_start_matches('`').trim())
                } else {
                    Fence::Closes
                }
            } else if inside {
                Fence::Inside
            } else {
                Fence::Outside
            };
            let out = Line {
                index,
                start,
                end,
                text: line,
                fence,
            };
            start = end;
            out
        })
}

/// `### 7.12 The facet model` → `(3, "7.12")`: the heading's depth and number.
///
/// Level one is the document title and is never cited. `## Appendix B: …`
/// carries no number and nothing cites it by one.
pub(crate) fn heading(line: &str) -> Option<(usize, &str)> {
    let depth = line.len() - line.trim_start_matches('#').len();
    if depth < 2 {
        return None;
    }
    let rest = line[depth..].strip_prefix(' ')?;
    let (number, written) = dotted_number(rest);
    // The number must be the whole first word: `## 2026-08 latency` is prose.
    if !rest[written..].is_empty() && !rest[written..].starts_with(' ') {
        return None;
    }
    number
        .starts_with(|c: char| c.is_ascii_digit())
        .then_some((depth, number))
}

/// `3. **Storage colocated with compute…**` → `(3, "**Storage…")`: a top-level
/// ordered item's number and the text after it. Indented items belong to a
/// nested list and are not top-level.
pub(crate) fn list_item(line: &str) -> Option<(u32, &str)> {
    let end = line
        .find(|c: char| !c.is_ascii_digit())
        .filter(|&e| e > 0 && line[e..].starts_with(". "))?;
    Some((line[..end].parse().ok()?, &line[end + 2..]))
}

/// The dotted number opening `text`, as a `§` or a heading writes it: `7.12.`
/// reads as `7.12`. Returns the number and how many bytes it was written in,
/// trailing period included; the number is empty when there is none.
pub(crate) fn dotted_number(text: &str) -> (&str, usize) {
    let written = text
        .find(|c: char| !c.is_ascii_digit() && c != '.')
        .unwrap_or(text.len());
    (text[..written].trim_end_matches('.'), written)
}

/// The backticked spans of one line, trimmed, empty ones skipped.
pub(crate) fn backticked(line: &str) -> impl Iterator<Item = &str> {
    let mut rest = line;
    std::iter::from_fn(move || {
        loop {
            let open = rest.find('`')?;
            let after = &rest[open + 1..];
            let close = after.find('`')?;
            let span = after[..close].trim();
            rest = &after[close + 1..];
            if !span.is_empty() {
                return Some(span);
            }
        }
    })
}

/// Up to `width` bytes of `line` either side of `line[from..to]`, trimmed, with
/// an ellipsis marking each side that was cut: enough to find a failure by eye.
pub(crate) fn excerpt(line: &str, from: usize, to: usize, width: usize) -> String {
    let start = floor_boundary(line, from.saturating_sub(width));
    let end = floor_boundary(line, (to + width).min(line.len()));
    let mut quote = String::new();
    if start > 0 {
        quote.push('…');
    }
    quote.push_str(line[start..end].trim());
    if end < line.len() {
        quote.push('…');
    }
    quote
}

fn floor_boundary(text: &str, mut index: usize) -> usize {
    while index > 0 && !text.is_char_boundary(index) {
        index -= 1;
    }
    index
}
