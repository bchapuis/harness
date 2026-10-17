//! Renumbering a specification without leaving its citations behind.
//!
//! A spec whose sections move is cited by number from the other specs and, far
//! more often, from the code. [`citations`] reports each
//! citation that lands nowhere, but a citation that lands on the *wrong* section
//! passes it, and a renumbering produces exactly those by the hundred. This module
//! rewrites them instead, from a [`Map`] of every old section to its new home.
//!
//! A rewrite reads the old numbering and writes the new one, so each citation is
//! resolved against the documents as they stood ([`OldAnchors`]) and rendered
//! against the documents as they are: a section that moved to another document
//! gains that document's prefix, and a citation whose inheritance would change
//! (a bare `§5` following a citation that just gained a prefix) is given its own.
//!
//! A document rewritten by hand already cites its own sections in the new
//! numbering, so a rewrite takes a set of *fresh* documents whose citations it
//! leaves alone: [`Context::fresh`].

use std::collections::BTreeMap;
use std::collections::BTreeSet;
use std::fmt;
use std::path::Path;

use crate::DOCUMENTS;
use crate::citations;
use crate::markdown;

/// Where every old section of the renumbered documents now lives.
#[derive(Debug, Default)]
pub struct Map {
    entries: BTreeMap<(&'static str, String), Target>,
    renumbered: BTreeSet<&'static str>,
}

/// The new home of one old section.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Target {
    /// The section now lives at this number of this document.
    Section(&'static str, String),
    /// The document was retired and the section has no home in the set; each
    /// citation of it is reported for a person to rewrite.
    Retired,
}

impl Map {
    /// Parse a map: one line per old section, tab-separated, `old-doc old-number
    /// new-doc new-number`, or `old-doc old-number -` for a section with no home.
    /// Blank lines and lines opening with `#` are skipped.
    pub fn parse(text: &str) -> Result<Self, String> {
        let mut map = Self::default();
        for (index, line) in text.lines().enumerate() {
            let line = line.trim();
            if line.is_empty() || line.starts_with('#') {
                continue;
            }
            let fields: Vec<&str> = line.split('\t').map(str::trim).collect();
            let at = |what: &str| format!("map line {}: {what}: {line:?}", index + 1);
            let old_doc = doc_id(fields.first().copied().unwrap_or(""))
                .ok_or_else(|| at("unknown old document"))?;
            let old = fields.get(1).ok_or_else(|| at("no old number"))?;
            let target = match fields.get(2..) {
                Some(["-"]) => Target::Retired,
                Some([doc, number]) => Target::Section(
                    doc_id(doc).ok_or_else(|| at("unknown new document"))?,
                    number.trim_start_matches('§').to_string(),
                ),
                _ => return Err(at("expected `new-doc new-number` or `-`")),
            };
            let old = old.trim_start_matches('§').to_string();
            if map.entries.insert((old_doc, old), target).is_some() {
                return Err(at("old section mapped twice"));
            }
            map.renumbered.insert(old_doc);
        }
        Ok(map)
    }

    /// Whether `doc`'s old sections appear in this map at all.
    pub fn renumbers(&self, doc: &str) -> bool {
        self.renumbered.contains(doc)
    }

    /// The new home of `doc`'s old section `number`, falling back to its nearest
    /// mapped ancestor: a list item `§5.4` whose leaf moved follows `§5`. The flag
    /// says whether the fallback was taken, which drops the suffix.
    pub fn lookup(&self, doc: &'static str, number: &str) -> Option<(Target, bool)> {
        let mut parts: Vec<&str> = number.split('.').collect();
        let mut exact = true;
        while !parts.is_empty() {
            if let Some(target) = self.entries.get(&(doc, parts.join("."))) {
                return Some((target.clone(), !exact));
            }
            parts.pop();
            exact = false;
        }
        None
    }

    /// The old sections of `doc` this map names, so a caller can hold it complete
    /// against the old document's headings.
    pub fn sections_of(&self, doc: &str) -> impl Iterator<Item = &str> {
        self.entries
            .keys()
            .filter(move |(d, _)| *d == doc)
            .map(|(_, n)| n.as_str())
    }
}

/// The anchors every document offered before the rewrite, so a bare citation in
/// old text resolves the way it did when it was written.
#[derive(Debug, Default)]
pub struct OldAnchors {
    by_doc: BTreeMap<&'static str, BTreeSet<String>>,
}

impl OldAnchors {
    /// Index the documents under `old_root`, a copy of the tree before the
    /// rewrite. A document absent there is new and offers no old anchors.
    pub fn load(old_root: &Path) -> std::io::Result<Self> {
        let mut by_doc = BTreeMap::new();
        for doc in DOCUMENTS {
            match std::fs::read_to_string(old_root.join(doc.path)) {
                Ok(text) => {
                    by_doc.insert(doc.id, citations::anchors(&text));
                }
                Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
                Err(e) => return Err(e),
            }
        }
        Ok(Self { by_doc })
    }

    /// Whether `doc` offered section `number` before the rewrite.
    pub fn offers(&self, doc: &str, number: &str) -> bool {
        self.by_doc.get(doc).is_some_and(|a| a.contains(number))
    }

    /// The old document's anchors, for holding a [`Map`] complete against them.
    pub fn of(&self, doc: &str) -> Option<&BTreeSet<String>> {
        self.by_doc.get(doc)
    }
}

/// How the citations of one text resolve, before and after.
#[derive(Debug, Clone, Copy)]
pub struct Context<'a> {
    /// Where a bare citation in the old text pointed: the documents tried in order.
    pub bare: &'a [&'static str],
    /// The document a bare citation names in the new text.
    pub default: Option<&'static str>,
    /// Documents this text already cites in the new numbering.
    pub fresh: &'a [&'static str],
}

/// What happened to one citation that did not simply follow its section.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Note {
    /// One-based line of the citation in the text.
    pub line: usize,
    /// What was found and what was done.
    pub what: String,
    /// The text around it.
    pub quote: String,
}

impl fmt::Display for Note {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}: {}\n    {}", self.line, self.what, self.quote)
    }
}

/// The outcome of rewriting one text.
#[derive(Debug, Default)]
pub struct Rewrite {
    /// The rewritten text.
    pub text: String,
    /// Citations moved to a new number or document.
    pub moved: usize,
    /// Citations that followed an ancestor and lost their suffix.
    pub fallbacks: Vec<Note>,
    /// Citations this module could not rewrite: a retired section, a section the
    /// map does not cover, or a link prefix naming another document.
    pub failures: Vec<Note>,
}

/// Rewrite every citation in `text` from the old numbering to the new.
///
/// `scan` is the text the citations are read from and must be byte-for-byte as
/// long as `text`: the text itself for a document, or its
/// [`rust_comments`](citations::rust_comments) mask for a source file.
pub fn rewrite(text: &str, scan: &str, ctx: Context<'_>, map: &Map, old: &OldAnchors) -> Rewrite {
    debug_assert_eq!(text.len(), scan.len());
    let mut out = Rewrite::default();
    // Edits as (start, end, replacement), applied back to front at the end.
    let mut edits: Vec<(usize, usize, String)> = Vec::new();
    let mut previous_end = 0usize;
    let mut previous_new: Option<&'static str> = None;

    for cite in citations::citations(scan) {
        let number_start = cite.offset + '§'.len_utf8();
        let (_, number_len) = markdown::dotted_number(&scan[number_start..]);
        let number_end = number_start + number_len;
        let separated = is_separator(&scan[previous_end..cite.offset]);
        previous_end = number_end;
        let note = |what: String| Note {
            line: cite.line,
            what,
            quote: cite.quote.clone(),
        };

        // Which document the old text meant.
        let old_doc = cite.target.or_else(|| {
            ctx.bare
                .iter()
                .copied()
                .find(|d| old.offers(d, &cite.number))
                .or_else(|| ctx.bare.first().copied())
        });
        let Some(old_doc) = old_doc else {
            previous_new = None;
            continue;
        };
        if ctx.fresh.contains(&old_doc) {
            previous_new = cite.target;
            continue;
        }

        // Where it lives now.
        let (new_doc, new_number) = if map.renumbers(old_doc) {
            match map.lookup(old_doc, &cite.number) {
                Some((Target::Section(doc, number), fallback)) => {
                    if fallback {
                        out.fallbacks.push(note(format!(
                            "{old_doc} §{} followed its ancestor to {doc} §{number}",
                            cite.number
                        )));
                    }
                    (doc, number)
                }
                Some((Target::Retired, _)) => {
                    out.failures.push(note(format!(
                        "{old_doc} §{} has no home in the new set",
                        cite.number
                    )));
                    previous_new = cite.target;
                    continue;
                }
                None => {
                    out.failures.push(note(format!(
                        "{old_doc} §{} is not covered by the map",
                        cite.number
                    )));
                    previous_new = cite.target;
                    continue;
                }
            }
        } else {
            (old_doc, cite.number.clone())
        };

        // How the new text must say it.
        if let Some(span) = cite.prefix.clone() {
            if new_doc != old_doc {
                if scan[span.clone()].starts_with('[') {
                    out.failures.push(note(format!(
                        "a link prefix names {old_doc}, but §{} moved to {new_doc}",
                        cite.number
                    )));
                } else {
                    edits.push((span.start, span.end, alias(new_doc).to_string()));
                }
            }
            previous_new = Some(new_doc);
        } else {
            let unchanged = new_doc == old_doc && new_number == cite.number;
            // A citation inherits in the new text when only punctuation follows a
            // citation that names a document there. In the old text that was only
            // possible if it inherited already; otherwise the one before it has
            // just gained a prefix.
            let needs_prefix = if separated && previous_new.is_some() {
                previous_new != Some(new_doc)
            } else {
                !unchanged && Some(new_doc) != ctx.default
            };
            if needs_prefix {
                edits.push((cite.offset, cite.offset, format!("{} ", alias(new_doc))));
                previous_new = Some(new_doc);
            } else if !(separated && previous_new.is_some()) {
                previous_new = None;
            }
        }
        if new_number != cite.number || new_doc != old_doc {
            out.moved += 1;
        }
        if new_number != cite.number {
            // Keep a trailing period the parser excluded from the number.
            let written = &scan[number_start..number_end];
            let suffix = &written[cite.number.len()..];
            edits.push((number_start, number_end, format!("{new_number}{suffix}")));
        }
    }

    let mut text = text.to_string();
    edits.sort_by(|a, b| b.0.cmp(&a.0).then(b.1.cmp(&a.1)));
    for (start, end, replacement) in edits {
        text.replace_range(start..end, &replacement);
    }
    out.text = text;
    out
}

/// The same reading of a gap as the citation parser's: only punctuation, `and`,
/// or `or` between two citations makes the second inherit the first's document.
fn is_separator(gap: &str) -> bool {
    let residue: String = gap
        .chars()
        .filter(|c| !c.is_whitespace() && !matches!(c, '/' | ',' | '–' | '-' | '&' | '(' | ')'))
        .collect();
    residue.is_empty() || residue.eq_ignore_ascii_case("and") || residue.eq_ignore_ascii_case("or")
}

fn doc_id(name: &str) -> Option<&'static str> {
    DOCUMENTS.iter().find(|d| d.id == name).map(|d| d.id)
}

/// The prefix a rewrite writes for `doc`: its first alias.
fn alias(doc: &str) -> &'static str {
    DOCUMENTS
        .iter()
        .find(|d| d.id == doc)
        .and_then(|d| d.aliases.first().copied())
        .unwrap_or("")
}

#[cfg(test)]
mod tests {
    use super::*;

    fn old() -> OldAnchors {
        let mut by_doc = BTreeMap::new();
        by_doc.insert(
            "entity",
            ["4", "4.1", "6", "6.2", "7", "7.4", "7.11"]
                .map(String::from)
                .into(),
        );
        by_doc.insert("actor", ["18", "18.1"].map(String::from).into());
        OldAnchors { by_doc }
    }

    fn map() -> Map {
        Map::parse(
            "entity\t4\tentity\t2\nentity\t4.1\tentity\t2.1\nentity\t6\tentity\t3\n\
             entity\t6.2\tentity\t3.2\nentity\t7\tentity\t4\nentity\t7.4\tentity\t4.3\n\
             entity\t7.11\tactor\t9\n",
        )
        .expect("parses")
    }

    fn run(text: &str, default: &'static str) -> Rewrite {
        let ctx = Context {
            bare: &[default],
            default: Some(default),
            fresh: &[],
        };
        rewrite(text, text, ctx, &map(), &old())
    }

    #[test]
    fn a_prefixed_citation_follows_its_section() {
        assert_eq!(run("see entity §4.1.", "actor").text, "see entity §2.1.");
    }

    #[test]
    fn a_bare_citation_follows_its_section() {
        assert_eq!(run("the fold (§6.2)", "entity").text, "the fold (§3.2)");
    }

    #[test]
    fn a_section_moved_to_another_document_gains_its_prefix() {
        assert_eq!(
            run("the facet (§7.11)", "entity").text,
            "the facet (actor §9)"
        );
        assert_eq!(run("entity §7.11", "actor").text, "actor §9");
    }

    #[test]
    fn an_inherited_citation_keeps_or_gains_its_prefix() {
        assert_eq!(run("entity §6, §6.2", "actor").text, "entity §3, §3.2");
        assert_eq!(
            run("entity §7.11, §7.4", "actor").text,
            "actor §9, entity §4.3"
        );
    }

    #[test]
    fn a_bare_citation_after_a_new_prefix_gets_its_own() {
        // `§7.4` was bare; after `actor §9` it would inherit actor.
        assert_eq!(
            run("(§7.11, §7.4)", "entity").text,
            "(actor §9, entity §4.3)"
        );
    }

    #[test]
    fn a_list_item_follows_its_section_and_says_so() {
        let out = run("entity §4.1.3 says", "actor");
        assert_eq!(out.text, "entity §2.1 says");
        assert_eq!(out.fallbacks.len(), 1);
    }

    #[test]
    fn a_bare_citation_that_did_not_move_is_left_bare() {
        // `§18.1` resolved through the bare list to actor and still does.
        let ctx = Context {
            bare: &["entity", "actor"],
            default: Some("entity"),
            fresh: &[],
        };
        let text = "the contract (§18.1)";
        assert_eq!(rewrite(text, text, ctx, &map(), &old()).text, text);
    }

    #[test]
    fn fresh_documents_are_left_alone() {
        let ctx = Context {
            bare: &["entity"],
            default: Some("entity"),
            fresh: &["entity"],
        };
        let text = "own §2, and actor §18.1";
        let out = rewrite(text, text, ctx, &map(), &old());
        assert_eq!(out.text, text);
    }

    #[test]
    fn an_unmapped_section_of_a_renumbered_document_fails() {
        let out = run("entity §9", "actor");
        assert_eq!(out.text, "entity §9");
        assert_eq!(out.failures.len(), 1);
    }
}
