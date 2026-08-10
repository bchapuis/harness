//! Cross-reference resolution over the specification set (`docs/README.md`).
//!
//! The specs address each other by section — `grain §7.12`, `actor §18.5`, a bare
//! `§9` inside the document that owns it. There are two thousand-odd such
//! citations across fourteen documents and nothing has ever checked one. A
//! renumbered section is the ordinary way they break: the edit is local, the
//! citations are not, and prose fails silently. The specs are layered precisely so
//! that each cites the ones beneath it, which makes the citation the load-bearing
//! part of the layering rather than a convenience.
//!
//! This crate resolves every citation against the document it names and reports
//! the ones that land nowhere. It is the `docs/` analogue of the per-crate
//! catalogue drift tests (`tests/conformance_catalogue.rs`), and it exists for the
//! same reason those do: a pointer that is only written down is a pointer that
//! rots.
//!
//! # What a document offers
//!
//! Three addressing schemes are in use, and [`anchors`] indexes all three:
//!
//! 1. **Numbered headings** — `## 7. Durability and replication`, `### 7.12 The
//!    facet model`. A heading also makes each of its ancestors addressable, so
//!    `§7.12` implies `§7` whether or not anything writes `## 7.` alone.
//! 2. **Numbered list items under a leaf section** — `DO §2.3` is the third item of
//!    `## 2. Key properties`, which carries no sub-headings. Indexed *only* for
//!    sections with no sub-heading, so a spec's real subsections are never shadowed
//!    by a list that happens to sit above them.
//! 3. **Bold inline labels** — the hardware envelope defines `hw §3.1` as a bolded
//!    run opening a paragraph inside `## 3.`, not as a heading of its own.
//!
//! Code fences are skipped when indexing anchors and *scanned* when collecting
//! citations: a doc comment quoted inside a spec cites sections like any other
//! prose, and those citations rot the same way.
//!
//! # What a citation names
//!
//! A citation is a `§` and a dotted number. What comes before it decides which
//! document it addresses, and the prose says so in four different ways — all four
//! are read here rather than pushed back onto the writing:
//!
//! - **A prefix is a word that names a registered document, and nothing else**
//!   ([`DocSpec::aliases`]). The alternative — treat whatever word precedes the `§`
//!   as a prefix — makes a prefix out of every `in §4.1` and `and §7.6`. Two words
//!   are examined, which is what lets `utilities spec §2.3` name the utilities spec
//!   while a bare `spec §6.1` stays unprefixed. The search crosses a line break,
//!   because a wrapped paragraph splits `sandbox` from its `spec §3.4`, but not a
//!   blank line.
//! - **A markdown link is a prefix.** The compatibility spec's boundary table
//!   writes `[granary](granary-spec.md) §7.12`, naming the document by link rather
//!   than by short prefix.
//! - **A prefix carries across separators.** `actor §18.5/§18.6` cites the actor
//!   spec twice and only the first says so. The inheritance is this crate's
//!   inference, but it binds like a written prefix, which means a citation that
//!   changes document mid-list has to say so. One did — `grain §6, §6.2`, where
//!   the second is the harness's own — and saying so was the cheaper fix.
//! - **An unprefixed citation follows the citing document's own convention**,
//!   declared per document as [`Unprefixed`]. Most specs number their own sections
//!   and mean themselves. Some number nothing and mean one particular spec
//!   throughout: `simulation-testing` is written against the actor spec's §16–§18,
//!   and every bare `§18.1` in it is an actor citation.
//!
//! # What is not checked
//!
//! Citations of documents outside the tree ([`EXTERNAL_PREFIXES`]) are skipped —
//! `Raft §3.10` is a section of the Raft paper and this crate has no copy of it.
//! An unrecognized prefix is treated as prose rather than as a typo, so a
//! misspelled `granray §5` resolves against the citing document instead of being
//! reported; it will usually fail there anyway, but that is a consequence rather
//! than a guarantee. Nothing here checks that a cited section says what the citing
//! sentence claims — only that it exists.

pub mod catalogue;
pub mod identifiers;

use std::collections::BTreeMap;
use std::collections::BTreeSet;
use std::fmt;
use std::io;
use std::path::Path;
use std::path::PathBuf;

/// Prefixes that name a document outside this tree. Their citations are skipped.
pub const EXTERNAL_PREFIXES: &[&str] = &["Raft"];

/// The workspace root, from a crate's `CARGO_MANIFEST_DIR`.
///
/// Every member sits at `crates/<name>`, so the root is two levels up. Tests read
/// `docs/` relative to it: `spec_xref::workspace_root(env!("CARGO_MANIFEST_DIR"))`.
///
/// # Panics
///
/// If `manifest_dir` is not two levels below a root, which for a workspace member
/// means the layout changed and every caller's paths are wrong anyway.
pub fn workspace_root(manifest_dir: &str) -> PathBuf {
    Path::new(manifest_dir)
        .ancestors()
        .nth(2)
        .expect("a workspace member sits two levels below the root")
        .to_path_buf()
}

/// How a document's *unprefixed* citations are resolved.
///
/// Every document declares one. The declaration is the convention that document
/// actually follows, so a citation resolving nowhere is a defect in the prose
/// rather than a gap in this table.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Unprefixed {
    /// The document numbers its own sections and a bare `§N` means its own §N.
    /// The ordinary case: every layered spec that cites outward with a prefix.
    Own,
    /// The document numbers its own sections and also cites others without a
    /// prefix. Its own sections are tried first, then the named documents in
    /// order.
    OwnThen(&'static [&'static str]),
    /// The document numbers nothing of its own; every bare `§N` belongs to one of
    /// the named documents.
    Other(&'static [&'static str]),
}

/// A document in the specification set: where it lives, what prefixes name it,
/// and how its own unprefixed citations resolve.
#[derive(Debug, Clone, Copy)]
pub struct DocSpec {
    /// Short stable handle, used in failure messages and by [`Unprefixed`].
    pub id: &'static str,
    /// Path relative to the repository root. Its file name also resolves a
    /// markdown-link prefix, so `[granary](granary-spec.md) §7.12` finds it.
    pub path: &'static str,
    /// The prefixes other documents use to name this one. Matched
    /// case-insensitively, so a sentence may open with `Actor §7`.
    pub aliases: &'static [&'static str],
    /// How this document's own unprefixed citations resolve.
    pub unprefixed: Unprefixed,
}

/// Every document that participates in the citation graph.
///
/// A spec absent from this table is checked by nothing, so
/// `tests/cross_references.rs` asserts the table covers all of `docs/` — adding a
/// spec without registering it fails the build, the way adding an invariant
/// without cataloguing it does.
pub const DOCUMENTS: &[DocSpec] = &[
    DocSpec {
        id: "actor",
        path: "docs/distributed-actor-spec.md",
        aliases: &["actor", "core"],
        unprefixed: Unprefixed::Own,
    },
    DocSpec {
        id: "utilities",
        path: "docs/cluster-utilities-spec.md",
        aliases: &["utilities", "util", "cluster-utilities"],
        unprefixed: Unprefixed::Own,
    },
    DocSpec {
        id: "wal",
        path: "docs/wal-spec.md",
        aliases: &["wal"],
        unprefixed: Unprefixed::Own,
    },
    DocSpec {
        id: "granary",
        path: "docs/granary-spec.md",
        aliases: &["granary", "grain"],
        unprefixed: Unprefixed::Own,
    },
    DocSpec {
        id: "blob",
        path: "docs/blob-store-spec.md",
        aliases: &["blob", "blob-store"],
        unprefixed: Unprefixed::Own,
    },
    DocSpec {
        id: "harness",
        path: "docs/agentic-harness-spec.md",
        aliases: &["harness"],
        unprefixed: Unprefixed::Own,
    },
    // §5 relates the tier model to the harness's own §5, and cites into it bare.
    DocSpec {
        id: "sandbox",
        path: "docs/sandbox-spec.md",
        aliases: &["sandbox"],
        unprefixed: Unprefixed::OwnThen(&["harness"]),
    },
    DocSpec {
        id: "machine",
        path: "docs/machine-spec.md",
        aliases: &["machine"],
        unprefixed: Unprefixed::Own,
    },
    DocSpec {
        id: "compatibility",
        path: "docs/compatibility-spec.md",
        aliases: &["compatibility", "compat"],
        unprefixed: Unprefixed::Own,
    },
    DocSpec {
        id: "hw",
        path: "docs/hardware-envelope.md",
        aliases: &["hw", "hardware-envelope"],
        unprefixed: Unprefixed::Own,
    },
    // Numbers nothing of its own, and is written against the actor spec's §16–§18
    // (see its opening paragraph): every bare `§N` in it is an actor citation. It
    // cites granary as well, and says so with a prefix each time — this list stays
    // one document long so that remains true rather than becoming a convention a
    // reader has to infer per sentence.
    DocSpec {
        id: "simulation",
        path: "docs/simulation-testing.md",
        aliases: &["simulation-testing"],
        unprefixed: Unprefixed::Other(&["actor"]),
    },
    // A deployment guide for the harness; its bare `spec §6.1` is the harness spec.
    DocSpec {
        id: "standalone",
        path: "docs/standalone-deployment.md",
        aliases: &["standalone-deployment"],
        unprefixed: Unprefixed::Other(&["harness"]),
    },
    DocSpec {
        id: "edge",
        path: "docs/multi-tenant-edge.md",
        aliases: &["multi-tenant-edge"],
        unprefixed: Unprefixed::Other(&["harness"]),
    },
    DocSpec {
        id: "docs-index",
        path: "docs/README.md",
        aliases: &[],
        unprefixed: Unprefixed::Other(&["actor"]),
    },
    // The rubric the specs were written against; its bare citations are the actor
    // spec's, which is the document its examples are drawn from.
    DocSpec {
        id: "principles",
        path: "design-principles.md",
        aliases: &["design-principles"],
        unprefixed: Unprefixed::Other(&["actor"]),
    },
    // A background note on Durable Objects that maps them onto this tree, so it
    // cites the actor spec's control-plane modes without a prefix.
    DocSpec {
        id: "DO",
        path: "research/durable-objects.md",
        aliases: &["DO"],
        unprefixed: Unprefixed::OwnThen(&["actor"]),
    },
    DocSpec {
        id: "durable-sqlite",
        path: "research/durable-sqlite-and-filesystem.md",
        aliases: &["durable-sqlite-and-filesystem"],
        unprefixed: Unprefixed::OwnThen(&["granary"]),
    },
];

/// A `§` citation as it appears in a document.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Citation {
    /// One-based line number in the citing document.
    pub line: usize,
    /// The [`DocSpec::id`] the citation names, if it named one.
    pub target: Option<&'static str>,
    /// Whether [`Citation::target`] was inherited from the citation before it
    /// rather than written at this one. Both bind; the flag records which the
    /// prose said out loud, so a failure can be read against what is written.
    pub inherited: bool,
    /// The dotted section number, without the `§` and without a trailing period.
    pub number: String,
    /// Enough surrounding text to find the citation by eye.
    pub quote: String,
}

/// A citation that resolves against none of the documents it could name.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Unresolved {
    /// The [`DocSpec::id`] of the citing document.
    pub from: &'static str,
    /// One-based line number in the citing document.
    pub line: usize,
    /// The section number as written, without the `§`.
    pub number: String,
    /// The documents whose anchors were searched, in order.
    pub tried: Vec<&'static str>,
    /// Enough surrounding text to find the citation by eye.
    pub quote: String,
}

impl fmt::Display for Unresolved {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "{}:{}: §{} exists in none of [{}]\n    {}",
            self.from,
            self.line,
            self.number,
            self.tried.join(", "),
            self.quote,
        )
    }
}

/// One document, indexed: the sections it offers and the citations it makes.
#[derive(Debug)]
pub struct Document {
    /// The table entry this document was loaded from.
    pub spec: &'static DocSpec,
    /// Where it was read from.
    pub path: PathBuf,
    /// Every section number this document makes addressable.
    pub anchors: BTreeSet<String>,
    /// Every `§` citation it makes, in source order.
    pub citations: Vec<Citation>,
}

/// The whole specification set, indexed and ready to resolve against.
#[derive(Debug)]
pub struct Registry {
    documents: Vec<Document>,
    by_id: BTreeMap<&'static str, usize>,
}

impl Registry {
    /// Read and index every document in [`DOCUMENTS`], relative to `root`.
    ///
    /// A missing file is an error rather than a skip: the table names the
    /// documents that must exist, so a spec renamed out from under it fails here.
    pub fn load(root: &Path) -> io::Result<Self> {
        let mut documents = Vec::with_capacity(DOCUMENTS.len());
        let mut by_id = BTreeMap::new();

        for spec in DOCUMENTS {
            let path = root.join(spec.path);
            let text = std::fs::read_to_string(&path).map_err(|e| {
                io::Error::new(e.kind(), format!("reading {}: {e}", path.display()))
            })?;
            by_id.insert(spec.id, documents.len());
            documents.push(Document {
                spec,
                path,
                anchors: anchors(&text),
                citations: citations(&text),
            });
        }

        Ok(Self { documents, by_id })
    }

    /// The indexed documents, in table order.
    pub fn documents(&self) -> &[Document] {
        &self.documents
    }

    /// Total citations across the set — the number this gate is worth.
    pub fn citation_count(&self) -> usize {
        self.documents.iter().map(|d| d.citations.len()).sum()
    }

    /// Every citation that resolves against none of the documents it could name.
    pub fn unresolved(&self) -> Vec<Unresolved> {
        let mut out = Vec::new();
        for doc in &self.documents {
            for cite in &doc.citations {
                let tried = self.targets(doc, cite);
                let resolved = tried.iter().any(|id| {
                    self.anchors_of(id)
                        .is_some_and(|a| a.contains(&cite.number))
                });
                if !resolved {
                    out.push(Unresolved {
                        from: doc.spec.id,
                        line: cite.line,
                        number: cite.number.clone(),
                        tried,
                        quote: cite.quote.clone(),
                    });
                }
            }
        }
        out
    }

    /// The documents a citation could name, most specific first.
    ///
    /// A prefix is the only candidate, whether written or inherited: `grain §7.99`
    /// is wrong even if some other spec happens to have a §7.99, and a `§6.2`
    /// following `grain §6` across a comma is granary's too. Only an unprefixed
    /// citation falls to the citing document's own convention.
    fn targets(&self, doc: &Document, cite: &Citation) -> Vec<&'static str> {
        let own: Vec<&'static str> = match doc.spec.unprefixed {
            Unprefixed::Own => vec![doc.spec.id],
            Unprefixed::OwnThen(others) => std::iter::once(doc.spec.id)
                .chain(others.iter().copied())
                .collect(),
            Unprefixed::Other(others) => others.to_vec(),
        };
        let mut targets = match cite.target {
            Some(id) => return vec![id],
            None => own,
        };
        targets.dedup();
        targets
    }

    fn anchors_of(&self, id: &str) -> Option<&BTreeSet<String>> {
        self.by_id.get(id).map(|&i| &self.documents[i].anchors)
    }
}

/// Every section number `text` makes addressable.
///
/// See the module docs for the three schemes. Content inside fenced code blocks
/// is skipped: a `## 3.` in a shell transcript is a comment, not a section.
pub fn anchors(text: &str) -> BTreeSet<String> {
    let headings: Vec<(usize, String)> = fenced_lines(text)
        .filter_map(|(i, line, fenced)| (!fenced).then(|| heading_number(line).map(|n| (i, n)))?)
        .collect();

    let mut out = BTreeSet::new();
    for (_, number) in &headings {
        // A heading makes its ancestors addressable too: `§7.12` implies `§7`.
        let mut parts: Vec<&str> = number.split('.').collect();
        while !parts.is_empty() {
            out.insert(parts.join("."));
            parts.pop();
        }
    }

    // A section with no sub-heading of its own may address its numbered list items
    // instead (`DO §2.3`). Restricting this to leaves keeps a list that merely
    // *precedes* a spec's real subsections from inventing numbers beside them.
    let is_leaf = |n: &str| {
        !headings
            .iter()
            .any(|(_, o)| o.len() > n.len() && o.starts_with(n) && o.as_bytes()[n.len()] == b'.')
    };
    let mut current: Option<&str> = None;
    for (i, line, fenced) in fenced_lines(text) {
        if let Some((_, number)) = headings.iter().find(|(h, _)| *h == i) {
            current = is_leaf(number).then_some(number.as_str());
            continue;
        }
        if fenced {
            continue;
        }
        if let Some(section) = current
            && let Some(item) = list_item_number(line)
        {
            out.insert(format!("{section}.{item}"));
        }
        if let Some(label) = bold_label_number(line) {
            out.insert(label);
        }
    }

    out
}

/// Every `§` citation in `text`, in source order.
///
/// Fenced code is scanned rather than skipped: doc comments quoted in the specs
/// cite sections, and those citations rot like any other.
pub fn citations(text: &str) -> Vec<Citation> {
    let mut out: Vec<Citation> = Vec::new();
    // A prefix carries to the citations after it while only punctuation separates
    // them, so `actor §18.5/§18.6` names the actor spec twice.
    let mut previous_end = 0usize;
    let mut previous_target: Option<&'static str> = None;
    // Line numbers are counted forward from the last citation: positions only
    // increase, so the whole scan stays linear in the length of the document.
    let mut line = 1usize;
    let mut counted = 0usize;

    for (pos, _) in text.match_indices('§') {
        let after = &text[pos + '§'.len_utf8()..];
        let digits = &after[..after
            .find(|c: char| !c.is_ascii_digit() && c != '.')
            .unwrap_or(after.len())];
        let number = digits.trim_end_matches('.');
        if number.is_empty() {
            continue;
        }
        let before = &text[..pos];
        if names_external_document(before) {
            continue;
        }

        line += text[counted..pos].bytes().filter(|&b| b == b'\n').count();
        counted = pos;

        let written = prefix_before(before);
        let inherited = written.is_none()
            && previous_target.is_some()
            && is_separator(&text[previous_end..pos]);
        let target = if inherited { previous_target } else { written };

        previous_end = pos + '§'.len_utf8() + digits.len();
        previous_target = target;
        out.push(Citation {
            line,
            target,
            inherited,
            number: number.to_string(),
            quote: quote_around(text, pos),
        });
    }
    out
}

/// Lines paired with their index and whether they sit inside a fenced code block.
/// The fence line itself counts as fenced.
fn fenced_lines(text: &str) -> impl Iterator<Item = (usize, &str, bool)> {
    let mut inside = false;
    text.lines().enumerate().map(move |(i, line)| {
        let fence = line.trim_start().starts_with("```");
        if fence {
            inside = !inside;
            return (i, line, true);
        }
        (i, line, inside)
    })
}

/// `## 7. Durability` → `7`; `### 7.12 The facet model` → `7.12`.
///
/// Level one is the document title and is never cited. `## Appendix B: …` carries
/// no number and nothing cites it by one.
fn heading_number(line: &str) -> Option<String> {
    let rest = line.strip_prefix("##")?.trim_start_matches('#');
    let rest = rest.strip_prefix(' ')?;
    let end = rest
        .find(|c: char| !c.is_ascii_digit() && c != '.')
        .unwrap_or(rest.len());
    let (token, tail) = rest.split_at(end);
    // The number must be the whole first word: `## 2026 latency figures` is prose.
    if !tail.is_empty() && !tail.starts_with(' ') {
        return None;
    }
    let number = token.trim_end_matches('.');
    (!number.is_empty() && number.starts_with(|c: char| c.is_ascii_digit()))
        .then(|| number.to_string())
}

/// `3. **Storage colocated with compute…**` → `3`, for a top-level ordered item.
/// Indented items belong to a nested list and address nothing.
fn list_item_number(line: &str) -> Option<u32> {
    let end = line
        .find(|c: char| !c.is_ascii_digit())
        .filter(|&e| e > 0 && line[e..].starts_with(". "))?;
    line[..end].parse().ok()
}

/// `**hw §3.1 — Amortize round trips…**` → `3.1`.
///
/// The em dash is what separates a *definition* from a citation that happens to
/// fall inside bold text.
fn bold_label_number(line: &str) -> Option<String> {
    let rest = line.trim_start().strip_prefix("**")?;
    let pos = rest.find('§')?;
    let after = &rest[pos + '§'.len_utf8()..];
    let end = after
        .find(|c: char| !c.is_ascii_digit() && c != '.')
        .unwrap_or(after.len());
    let number = after[..end].trim_end_matches('.');
    if number.is_empty() || !after[end..].trim_start().starts_with('—') {
        return None;
    }
    Some(number.to_string())
}

/// Whether the word before the `§` names a document outside this tree.
fn names_external_document(before: &str) -> bool {
    trim_gap(before)
        .and_then(trailing_word)
        .is_some_and(|w| EXTERNAL_PREFIXES.iter().any(|e| e.eq_ignore_ascii_case(w)))
}

/// The document a citation names, by short prefix or by markdown link.
///
/// Two words are examined: `spec` is a noun that sits between a prefix and its
/// citation (`utilities spec §2.3`), and a bare `spec §6.1` names whatever the
/// citing document is about, which is the unprefixed case. Only a registered alias
/// counts, so the `in` of `the fold in §4.1` is prose.
fn prefix_before(before: &str) -> Option<&'static str> {
    let mut head = before;
    for _ in 0..2 {
        head = trim_gap(head)?;
        if let Some(id) = link_before(head) {
            return Some(id);
        }
        let word = trailing_word(head)?;
        if let Some(id) = alias_id(word) {
            return Some(id);
        }
        head = &head[..head.len() - word.len()];
    }
    None
}

/// Trim the whitespace separating a citation from what precedes it.
///
/// `None` when there is no whitespace at all (`(§7.2)` has no prefix) or when a
/// blank line intervenes: a prefix reaches across a wrapped line, not across a
/// paragraph.
fn trim_gap(head: &str) -> Option<&str> {
    let trimmed = head.trim_end_matches(char::is_whitespace);
    let gap = &head[trimmed.len()..];
    let paragraph_break = gap.bytes().filter(|&b| b == b'\n').count() > 1;
    (!gap.is_empty() && !paragraph_break).then_some(trimmed)
}

/// `…[granary](granary-spec.md)` → `granary`, matching the link target against
/// [`DocSpec::path`]. The compatibility spec's tables address documents this way.
fn link_before(head: &str) -> Option<&'static str> {
    let inner = head.strip_suffix(')')?;
    let open = inner.rfind("](")?;
    let target = inner[open + 2..].rsplit('/').next()?;
    DOCUMENTS
        .iter()
        .find(|d| d.path.rsplit('/').next() == Some(target))
        .map(|d| d.id)
}

/// The document a prefix word names, if any.
fn alias_id(word: &str) -> Option<&'static str> {
    DOCUMENTS
        .iter()
        .find(|d| d.aliases.iter().any(|a| a.eq_ignore_ascii_case(word)))
        .map(|d| d.id)
}

/// The run of word characters ending `text`, or `None` if it ends in punctuation.
fn trailing_word(text: &str) -> Option<&str> {
    let start = text
        .rfind(|c: char| !c.is_ascii_alphanumeric() && c != '-')
        .map_or(0, |i| {
            i + text[i..].chars().next().map_or(1, char::len_utf8)
        });
    (start < text.len()).then(|| &text[start..])
}

/// Whether only punctuation separates two citations, so the second inherits the
/// first's target: `§18.5/§18.6`, `§7, §8`, `§4 and §5`.
fn is_separator(gap: &str) -> bool {
    let residue: String = gap
        .chars()
        .filter(|c| !c.is_whitespace() && !matches!(c, '/' | ',' | '–' | '-' | '&' | '(' | ')'))
        .collect();
    residue.is_empty() || residue.eq_ignore_ascii_case("and") || residue.eq_ignore_ascii_case("or")
}

/// Enough of the line around `pos` to find the citation by eye in the source.
fn quote_around(text: &str, pos: usize) -> String {
    const WIDTH: usize = 48;
    let line_start = text[..pos].rfind('\n').map_or(0, |i| i + 1);
    let line_end = text[pos..].find('\n').map_or(text.len(), |i| pos + i);
    let start = floor_boundary(text, pos.saturating_sub(WIDTH).max(line_start));
    let end = floor_boundary(text, (pos + WIDTH).min(line_end));
    let mut quote = String::new();
    if start > line_start {
        quote.push('…');
    }
    quote.push_str(text[start..end].trim());
    if end < line_end {
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_heading_makes_itself_and_its_ancestors_addressable() {
        let a = anchors("## 7. Durability\n\n### 7.12 The facet model\n");
        assert!(a.contains("7"), "the section itself");
        assert!(a.contains("7.12"), "the subsection");
    }

    #[test]
    fn a_subsection_implies_its_parent_even_with_no_parent_heading() {
        let a = anchors("### 18.1 Determinism contract\n");
        assert!(
            a.contains("18"),
            "`§18` must resolve on the strength of §18.1"
        );
    }

    #[test]
    fn an_unnumbered_heading_addresses_nothing() {
        let a = anchors("## Appendix B: Suggested crate layout\n## Primitives\n");
        assert!(a.is_empty(), "got {a:?}");
    }

    #[test]
    fn a_fenced_heading_is_a_comment_not_a_section() {
        let a = anchors("```sh\n## 9. not a section\n```\n");
        assert!(a.is_empty(), "got {a:?}");
    }

    #[test]
    fn a_leaf_sections_list_items_are_addressable() {
        let a = anchors("## 2. Key properties\n\n1. **One.**\n2. **Two.**\n3. **Three.**\n");
        assert!(a.contains("2.3"), "DO §2.3 addresses the third item");
    }

    #[test]
    fn list_items_do_not_shadow_a_sections_real_subsections() {
        let text = "## 7. Durability\n\n1. first\n2. second\n\n### 7.1 Shards\n";
        let a = anchors(text);
        assert!(a.contains("7.1"), "the real subsection");
        assert!(
            !a.contains("7.2"),
            "a list above the subsections must not invent §7.2: got {a:?}",
        );
    }

    #[test]
    fn a_fenced_list_addresses_nothing() {
        let text = "## 6. The gates\n\n```\n1. decide\n2. append\n```\n";
        assert!(!anchors(text).contains("6.2"), "a code block is not a list");
    }

    #[test]
    fn a_bold_label_defines_a_section() {
        let a = anchors("## 3. What follows\n\n**hw §3.1 — Amortize round trips.** Body.\n");
        assert!(a.contains("3.1"), "got {a:?}");
    }

    #[test]
    fn a_bold_citation_without_an_em_dash_defines_nothing() {
        let a = anchors("## 3. What follows\n\n**See hw §3.9 for the arithmetic.**\n");
        assert!(!a.contains("3.9"), "got {a:?}");
    }

    fn cited(text: &str) -> Vec<(Option<&'static str>, bool, String)> {
        citations(text)
            .into_iter()
            .map(|c| (c.target, c.inherited, c.number))
            .collect()
    }

    fn targets(text: &str) -> Vec<Option<&'static str>> {
        citations(text).into_iter().map(|c| c.target).collect()
    }

    #[test]
    fn a_prefixed_citation_names_its_document() {
        assert_eq!(
            cited("routing (grain §7.12) is"),
            [(Some("granary"), false, "7.12".into())]
        );
    }

    #[test]
    fn a_parenthesized_citation_has_no_prefix() {
        assert_eq!(targets("the journal (§6.1)"), [None]);
    }

    #[test]
    fn a_trailing_period_is_not_part_of_the_number() {
        assert_eq!(
            cited("see granary §16."),
            [(Some("granary"), false, "16".into())]
        );
    }

    #[test]
    fn spec_is_skipped_in_favour_of_the_word_before_it() {
        assert_eq!(
            targets("placement is routing (utilities spec §2.3)"),
            [Some("utilities")]
        );
    }

    #[test]
    fn a_bare_spec_is_left_unprefixed_for_the_document_to_resolve() {
        assert_eq!(targets("one logical store (spec §6.1)"), [None]);
    }

    #[test]
    fn a_word_that_names_no_document_is_prose_not_a_prefix() {
        assert_eq!(targets("the fold in §4.1 is"), [None]);
    }

    #[test]
    fn punctuation_ends_the_search_for_a_prefix() {
        assert_eq!(
            targets("unlike granary, the §5 here"),
            [None],
            "a comma separates two clauses, not a prefix from its section",
        );
    }

    #[test]
    fn a_prefix_reaches_across_a_wrapped_line() {
        assert_eq!(
            targets("shared-kernel confinement (sandbox\n  spec §3.4's SHOULD grade)"),
            [Some("sandbox")],
        );
    }

    #[test]
    fn a_prefix_does_not_reach_across_a_paragraph() {
        assert_eq!(targets("about granary\n\n§7.2 says"), [None]);
    }

    #[test]
    fn a_markdown_link_names_a_document() {
        assert_eq!(
            targets("stamped by revision | [granary](granary-spec.md) §7.12 |"),
            [Some("granary")],
        );
    }

    #[test]
    fn a_markdown_link_with_a_code_span_label_still_names_it() {
        assert_eq!(
            targets("the transport ([`sandbox-spec.md`](sandbox-spec.md) §3.5), but"),
            [Some("sandbox")],
        );
    }

    #[test]
    fn a_link_to_something_unregistered_is_not_a_prefix() {
        assert_eq!(
            targets("see [the paper](https://example.com/x.pdf) §4"),
            [None]
        );
    }

    #[test]
    fn a_prefix_carries_across_a_separator_as_an_inherited_one() {
        assert_eq!(
            cited("the way actor §18.5/§18.6 prescribe"),
            [
                (Some("actor"), false, "18.5".into()),
                (Some("actor"), true, "18.6".into()),
            ],
        );
    }

    #[test]
    fn a_prefix_carries_across_a_comma_and_an_and() {
        assert_eq!(
            targets("granary §7.1, §7.2 and §7.6"),
            [Some("granary"), Some("granary"), Some("granary")],
        );
    }

    #[test]
    fn a_prefix_does_not_carry_across_prose() {
        let got = cited("actor §7 governs transport, and the fold in §4.1 does not");
        assert_eq!(got[0], (Some("actor"), false, "7".into()));
        assert_eq!(got[1], (None, false, "4.1".into()), "`in` is prose");
    }

    #[test]
    fn an_external_citation_is_skipped() {
        assert!(
            cited("without waiting out an election timeout (Raft §3.10)").is_empty(),
            "the Raft paper is not in this tree",
        );
    }

    #[test]
    fn a_citation_inside_a_doc_comment_is_still_a_citation() {
        assert_eq!(
            cited("/// rehydration (§9, G3/G4)."),
            [(None, false, "9".into())]
        );
    }

    #[test]
    fn an_invariant_number_is_not_a_citation() {
        assert!(cited("invariants #1–#22 and G1–G21").is_empty());
    }

    #[test]
    fn line_numbers_count_from_one() {
        let got = citations("first line\nsecond §4 line\n");
        assert_eq!(got[0].line, 2);
    }
}
