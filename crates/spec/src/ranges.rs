//! The invariant *ranges* the specs cite of each other, resolved against the
//! catalogues they name.
//!
//! A spec names a sibling's catalogue by its extent: `grain §N` (its invariants as
//! **G1–G20**), "the harness (H1–H8)", "core §18.5 #1–#22". The range is a
//! citation like any other — it points at a catalogue and says how far it runs —
//! but where [`crate::citations`] resolves a `§` against a heading and
//! [`crate::identifiers`] resolves a backticked name against the tree, nothing
//! resolved these. So they rot the same way, and quietly: a catalogue that *grows*
//! leaves every sibling that stated its old extent describing a shorter document
//! than the one on disk, and each of those sentences still reads exactly right.
//!
//! That is not hypothetical. `G21` was catalogued at granary §7.16 and every
//! citation of granary's extent went on saying `G1–G20` — six of them across four
//! documents: the harness and machine preambles, the machine's §7,
//! `docs/README.md`, and `simulation-testing.md`. Every one parses, resolves, and
//! passes every other gate.
//!
//! # What is checked
//!
//! Two directions, and neither is the obvious one.
//!
//! The obvious check — every number in the span exists — is *wrong here*: the
//! harness catalogue has no H2 (harness §11 retires it deliberately), so `H1–H8`
//! spans a number nothing defines and is still the correct way to write the
//! harness's extent. And the natural stronger check — a range is the whole
//! catalogue, so its high end is the last invariant — is wrong too, because
//! `M1–M3` in machine §7 is a genuine claim about three of six.
//!
//! What holds without exception is the pair:
//!
//! - **No range runs past the end.** A citation naming `G22` is naming an
//!   invariant no catalogue defines, whatever the surrounding sentence meant.
//! - **The end is named somewhere.** Across the whole document set, some range
//!   reaches the catalogue's last invariant. One spec's sub-span is another's
//!   full extent, so the union is what carries the claim — and a catalogue that
//!   grows past every citation of it is precisely the `G21` case.
//!
//! Together they pin the extent from both sides without a rule about what any one
//! sentence intended. The second is also what makes a catalogue nothing cites at
//! all fail rather than pass vacuously: `docs/README.md` rendered the wal
//! catalogue's column as `—` while `wal §7` held W1–W5 and
//! `crates/wal/tests/conformance_catalogue.rs` compared them, and the extent went
//! unstated by the entire set.

use std::collections::BTreeMap;
use std::fmt;
use std::io;
use std::path::Path;

use crate::DOCUMENTS;
use crate::catalogue::CATALOGUES;
use crate::catalogue::documented;

/// What a range label names.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Owner {
    /// A site in [`CATALOGUES`], by its [`crate::catalogue::CatalogueSite::id`].
    /// Its rows are parsed and the range is held against them.
    Catalogue(&'static str),
    /// A table the catalogue gates do not read, with the reason. Ranges carrying
    /// the label are scanned and then skipped — by a declaration naming what is
    /// unguarded, rather than by a scan that silently finds nothing.
    Unregistered(&'static str),
}

/// Every label a range citation can carry.
///
/// The letters are the catalogues' own (`G` for `G1`, `#` for the core's bare
/// numbering). A label absent from this table fails [`resolve`] rather than being
/// skipped, so a new catalogue is registered here as part of adding it — the way
/// a new spec is registered in [`DOCUMENTS`].
pub const LABELS: &[(&str, Owner)] = &[
    ("#", Owner::Catalogue("core")),
    ("U", Owner::Catalogue("utilities")),
    ("W", Owner::Catalogue("wal")),
    ("G", Owner::Catalogue("granary")),
    ("B", Owner::Catalogue("blob")),
    ("H", Owner::Catalogue("harness")),
    ("S", Owner::Catalogue("sandbox")),
    ("M", Owner::Catalogue("machine")),
    (
        "F",
        Owner::Unregistered(
            "granary §7.12's facet contract: four rows beside the G catalogue rather \
             than a site of its own, and no crate holds a Rust copy of it",
        ),
    ),
    (
        "V",
        Owner::Unregistered(
            "compatibility §2: a two-column table with neither a `Defined in` nor a \
             `Verified by`, so there is no Rust copy to compare it against — V3 is \
             held at compile time by `compat::Window` and V4/V5 by the golden corpus \
             (compatibility §4)",
        ),
    ),
];

/// One `G1–G20` as written, with where it was written.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Range {
    /// The label both endpoints carry: `G`, `#`.
    pub label: String,
    /// The first invariant named.
    pub low: u8,
    /// The last invariant named.
    pub high: u8,
    /// 1-based line within the document.
    pub line: usize,
    /// The text as written, for the failure message.
    pub text: String,
}

impl fmt::Display for Range {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.text)
    }
}

/// A range citation that names no catalogue this table knows.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct UnknownLabel {
    /// The document it was written in, by [`crate::DocSpec::path`].
    pub path: &'static str,
    /// The range as written.
    pub range: Range,
}

/// The owner of `label`, or `None` if [`LABELS`] does not list it.
pub fn resolve(label: &str) -> Option<Owner> {
    LABELS
        .iter()
        .find(|(l, _)| *l == label)
        .map(|(_, owner)| *owner)
}

/// Every invariant range in `text`, in source order.
///
/// A range is two same-labelled numbers around a dash: `G1–G20`, `#1–#22`,
/// `H3–H8`. Both the en dash the specs use and a plain hyphen are accepted, since
/// which one a sentence carries is a typographic accident and neither should
/// change what a gate sees.
///
/// The label must be present on *both* sides, which is what keeps `2026-08` and
/// `§18.5–§18.6` out: a bare number pair carries no label, and `§` is not one.
pub fn ranges(text: &str) -> Vec<Range> {
    let bytes = text.as_bytes();
    let mut out = Vec::new();
    // Line numbers are counted forward from the last match, so the whole scan
    // stays linear in the length of the document.
    let mut line = 1usize;
    let mut counted = 0usize;

    for (dash, sep) in dashes(text) {
        // Left of the dash: digits, then the label that prefixes them.
        let low_start = walk_back(bytes, dash, u8::is_ascii_digit);
        if low_start == dash {
            continue;
        }
        let label_start = walk_back(bytes, low_start, is_label_byte);
        if label_start == low_start {
            continue;
        }
        // Right of the dash: the same label, then digits.
        let after = dash + sep.len_utf8();
        let label = &text[label_start..low_start];
        let Some(rest) = text[after..].strip_prefix(label) else {
            continue;
        };
        let high_end = after + label.len() + walk_forward(rest.as_bytes(), u8::is_ascii_digit);
        if high_end == after + label.len() {
            continue;
        }

        let (Ok(low), Ok(high)) = (
            text[low_start..dash].parse::<u8>(),
            text[after + label.len()..high_end].parse::<u8>(),
        ) else {
            continue;
        };

        line += text[counted..label_start]
            .bytes()
            .filter(|&b| b == b'\n')
            .count();
        counted = label_start;

        out.push(Range {
            label: label.to_string(),
            low,
            high,
            line,
            text: text[label_start..high_end].to_string(),
        });
    }
    out
}

/// The extent of every registered catalogue: its lowest and highest row number.
///
/// Read from the specifications, so this is the same copy the drift tests compare
/// the Rust catalogues against.
pub fn extents(root: &Path) -> io::Result<BTreeMap<&'static str, (u8, u8)>> {
    let mut out = BTreeMap::new();
    for site in CATALOGUES {
        let rows = documented(root, site)?;
        let (Some(low), Some(high)) = (
            rows.iter().map(|r| r.number).min(),
            rows.iter().map(|r| r.number).max(),
        ) else {
            return Err(io::Error::other(format!(
                "{}: §{} of the {} spec yielded no catalogue rows",
                site.id, site.section, site.doc,
            )));
        };
        out.insert(site.id, (low, high));
    }
    Ok(out)
}

/// Every range citation across [`DOCUMENTS`], paired with the document it sits in.
pub fn scan(root: &Path) -> io::Result<Vec<(&'static str, Range)>> {
    let mut out = Vec::new();
    for doc in DOCUMENTS {
        let path = root.join(doc.path);
        let text = std::fs::read_to_string(&path)
            .map_err(|e| io::Error::new(e.kind(), format!("reading {}: {e}", path.display())))?;
        out.extend(ranges(&text).into_iter().map(|r| (doc.path, r)));
    }
    Ok(out)
}

/// Range citations whose label [`LABELS`] does not list.
pub fn unknown_labels(scanned: &[(&'static str, Range)]) -> Vec<UnknownLabel> {
    scanned
        .iter()
        .filter(|(_, r)| resolve(&r.label).is_none())
        .map(|(path, range)| UnknownLabel {
            path,
            range: range.clone(),
        })
        .collect()
}

/// The label a catalogue's rows carry, as [`LABELS`] spells it.
pub fn label_of(catalogue: &str) -> Option<&'static str> {
    LABELS.iter().find_map(|(label, owner)| {
        matches!(owner, Owner::Catalogue(id) if *id == catalogue).then_some(*label)
    })
}

fn dashes(text: &str) -> impl Iterator<Item = (usize, char)> + '_ {
    text.char_indices().filter(|(_, c)| *c == '–' || *c == '-')
}

fn is_label_byte(b: &u8) -> bool {
    b.is_ascii_uppercase() || *b == b'#'
}

fn walk_back(bytes: &[u8], from: usize, mut accept: impl FnMut(&u8) -> bool) -> usize {
    let mut at = from;
    while at > 0 && accept(&bytes[at - 1]) {
        at -= 1;
    }
    at
}

fn walk_forward(bytes: &[u8], mut accept: impl FnMut(&u8) -> bool) -> usize {
    let mut at = 0;
    while at < bytes.len() && accept(&bytes[at]) {
        at += 1;
    }
    at
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn reads_a_range_with_either_dash() {
        let found = ranges("its invariants as **G1–G20**, and the harness (H1-H8)");
        assert_eq!(found.len(), 2);
        assert_eq!(
            (found[0].label.as_str(), found[0].low, found[0].high),
            ("G", 1, 20)
        );
        assert_eq!(
            (found[1].label.as_str(), found[1].low, found[1].high),
            ("H", 1, 8)
        );
        assert_eq!(found[0].text, "G1–G20");
    }

    #[test]
    fn reads_the_cores_bare_numbering() {
        let found = ranges("core §18.5 #1–#22");
        assert_eq!(found.len(), 1);
        assert_eq!(
            (found[0].label.as_str(), found[0].low, found[0].high),
            ("#", 1, 22)
        );
    }

    /// A sub-span is a range like any other; deciding what it *claims* is the
    /// gate's job, not the scanner's.
    #[test]
    fn reads_a_range_that_does_not_start_at_one() {
        let found = ranges("so M1–M3 cover it unchanged, and H3–H8 keep stable numbers");
        assert_eq!(
            found
                .iter()
                .map(|r| (r.label.as_str(), r.low, r.high))
                .collect::<Vec<_>>(),
            [("M", 1, 3), ("H", 3, 8)],
        );
    }

    /// The label is what separates a citation from a date, a section span, or a
    /// page number. Without it on both sides there is nothing to resolve.
    #[test]
    fn skips_number_pairs_that_carry_no_label() {
        assert!(ranges("2026-08-11 and §18.5–§18.6 and 1–22").is_empty());
    }

    #[test]
    fn skips_a_pair_whose_labels_disagree() {
        assert!(ranges("G1–H20").is_empty());
    }

    #[test]
    fn counts_lines_across_several_ranges() {
        let found = ranges("G1–G21\n\n\nS1–S5\nM1–M6");
        assert_eq!(found.iter().map(|r| r.line).collect::<Vec<_>>(), [1, 4, 5]);
    }

    #[test]
    fn every_registered_catalogue_has_a_label() {
        for site in CATALOGUES {
            assert!(
                label_of(site.id).is_some(),
                "{}: no entry in LABELS, so ranges naming it resolve nowhere",
                site.id,
            );
        }
    }
}
