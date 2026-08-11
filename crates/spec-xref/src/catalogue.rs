//! The invariant catalogues, read out of the specifications.
//!
//! Each spec carries an invariant catalogue, and each is *also* written down in
//! Rust beside the suite that verifies it — `g_catalogue()`, `b_catalogue()`,
//! `core_catalogue()`, and their siblings. The Rust table is what the per-crate
//! `conformance_catalogue.rs` drift test already guards: it holds the numbering
//! complete and every test file it points at real.
//!
//! What nothing guarded until now is that the *two tables agree*. They are two
//! hand-maintained copies of one fact, and an invariant added to one, a section
//! moved in one, or a suite renamed in one leaves the other quietly wrong — the
//! failure the Rust table was introduced to prevent, reappearing one level up.
//!
//! This module reads the specification's copy so a crate's drift test can compare
//! the two. It reads rather than generates because the prose is not derivable: a
//! spec row states the invariant in full, with its reasoning, where the Rust
//! `property` field is a one-line summary. Only the load-bearing parts are
//! compared — which invariants exist, which sections define them, and which
//! suites verify them.
//!
//! # The two shapes, and four vocabularies
//!
//! Seven catalogues are markdown tables with a `Verified by` column. The core
//! catalogue (actor §18.5) is a numbered list instead ([`Shape`]).
//!
//! The `Verified by` columns are not written alike, which is worth knowing before
//! trusting one: granary, utilities, and sandbox name test **files**, wal names
//! test **functions**, blob and machine name functions where their Rust
//! catalogues name files, and harness describes the verification in prose. Only
//! matching vocabularies can be compared, and [`Pointers`] records which is which
//! per site — so where the suite axis is skipped, it is skipped by a declaration
//! naming the reason rather than by a comparison that finds nothing.
//!
//! # What is compared, and what is not
//!
//! [`compare`] reports three kinds of disagreement: an invariant in one table and
//! not the other, a different set of defining sections, and a different set of
//! verifying suites. It deliberately does not compare the prose, and it does not
//! see a `Verify::Checker` or a `Verify::CompileTime` — those name a checker or a
//! trait bound rather than a file, and a spec row mentions them in prose where it
//! mentions them at all. Continuous checkers have their own cross-check in
//! `actor_simulation::checker_coverage`.

use std::collections::BTreeSet;
use std::fmt;
use std::io;
use std::path::Path;

use crate::DOCUMENTS;

/// How a specification writes its invariant catalogue.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Shape {
    /// A markdown table with `Invariant`, `Defined in`, and `Verified by`
    /// columns, the invariant's label in the first cell.
    Table,
    /// A numbered list, each item opening `N. **Property (§a, §b).**`. Carries no
    /// verifying suites, so [`Row::pointers`] is empty for every row.
    NumberedList,
}

/// What the `Verified by` column names, and so what can be compared against the
/// Rust catalogue's `verify` entries.
///
/// The columns are not written in one vocabulary. Three of the eight cannot be
/// compared at all, and [`Prose`](Pointers::Prose) says so at the site rather than
/// leaving the comparison quietly empty.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Pointers {
    /// Test files, `` `grains.rs` `` or `` `harness-sandbox/tests/workspace.rs` ``.
    /// Compared by stem, so the path a spec writes matches the bare file name a
    /// catalogue writes, and the `compile_fail.rs` runner matches the
    /// `compile_fail` case directory beside it.
    Files,
    /// Test functions, `` `a_torn_tail_is_discarded_and_appends_continue` `` —
    /// what the wal catalogue points at, its suite living in `src/lib.rs`.
    Functions,
    /// The two copies do not name the same kind of thing, so there is nothing to
    /// compare. Each site says why. Identity and defining sections are still
    /// compared; only the suite axis is skipped.
    Prose,
}

/// Where one invariant catalogue lives in the specifications.
#[derive(Debug, Clone, Copy)]
pub struct CatalogueSite {
    /// Short handle, used in failure messages: `granary`, `core`.
    pub id: &'static str,
    /// The [`crate::DocSpec::id`] of the spec that carries it.
    pub doc: &'static str,
    /// The section number the catalogue lives under, e.g. `15`.
    pub section: &'static str,
    /// The letter the labels carry: `G` for `G1`, empty for the core's bare `1.`.
    pub label: &'static str,
    /// How the specification writes it.
    pub shape: Shape,
    /// What its `Verified by` column names.
    pub pointers: Pointers,
}

/// Every invariant catalogue in the tree, and where its specification copy lives.
///
/// The Rust copy is *not* named here: it lives in a `tests/` module that only its
/// own crate can import, which is why the comparison runs inside each crate's
/// `conformance_catalogue.rs` rather than centrally.
pub const CATALOGUES: &[CatalogueSite] = &[
    // A numbered list rather than a table, and it names no suites at all: §18.6
    // describes the layering instead. Identity and sections only.
    CatalogueSite {
        id: "core",
        doc: "actor",
        section: "18.5",
        label: "",
        shape: Shape::NumberedList,
        pointers: Pointers::Prose,
    },
    CatalogueSite {
        id: "utilities",
        doc: "utilities",
        section: "6",
        label: "U",
        shape: Shape::Table,
        pointers: Pointers::Files,
    },
    CatalogueSite {
        id: "wal",
        doc: "wal",
        section: "7",
        label: "W",
        shape: Shape::Table,
        pointers: Pointers::Functions,
    },
    CatalogueSite {
        id: "granary",
        doc: "granary",
        section: "15",
        label: "G",
        shape: Shape::Table,
        pointers: Pointers::Files,
    },
    CatalogueSite {
        id: "blob",
        doc: "blob",
        section: "9",
        label: "B",
        shape: Shape::Table,
        pointers: Pointers::Functions,
    },
    // The spec describes verification in prose — "differential resume-vs-
    // uninterrupted test; seed-reproducibility sweep" — naming no suite a build
    // could resolve.
    CatalogueSite {
        id: "harness",
        doc: "harness",
        section: "11",
        label: "H",
        shape: Shape::Table,
        pointers: Pointers::Prose,
    },
    CatalogueSite {
        id: "sandbox",
        doc: "sandbox",
        section: "6",
        label: "S",
        shape: Shape::Table,
        pointers: Pointers::Files,
    },
    CatalogueSite {
        id: "machine",
        doc: "machine",
        section: "7",
        label: "M",
        shape: Shape::Table,
        pointers: Pointers::Functions,
    },
];

/// One catalogue row, reduced to the parts both copies state.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Row {
    /// The number within its catalogue: `7` for `G7`.
    pub number: u8,
    /// The sections that define it, as written: `["6", "8"]` for `§6, §8`.
    pub sections: Vec<String>,
    /// The suites that verify it, by stem.
    pub pointers: BTreeSet<String>,
}

impl Row {
    /// Build the implementation's side of a row from a Rust catalogue entry.
    ///
    /// `spec` is the entry's section list in any of the forms the tables use
    /// (`§6, §8`, `granary §6, §8`, `wal §3.2-§3.4`) — only the numbers are kept.
    ///
    /// `verify` is the text of the entry's `Verify` entries, passed whole. A
    /// caller need not sort them: several are prose with the file names embedded
    /// (`"harness-sandbox/tests/workspace.rs (adversarial traversal); tests/
    /// native.rs (container confinement smoke)"`), and a `Verify::Checker` or
    /// `Verify::CompileTime` names no suite at all. `kind` decides which tokens
    /// are pointers and the rest are ignored, exactly as they are in the spec's
    /// own cell.
    pub fn new<I, S>(number: u8, spec: &str, kind: Pointers, verify: I) -> Self
    where
        I: IntoIterator<Item = S>,
        S: AsRef<str>,
    {
        Self {
            number,
            sections: section_numbers(spec),
            pointers: verify
                .into_iter()
                .flat_map(|text| pointer_tokens(text.as_ref(), kind))
                .collect(),
        }
    }
}

/// A disagreement between a specification's catalogue and its Rust copy.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Mismatch {
    /// The specification lists an invariant the Rust catalogue does not.
    Undocumented {
        /// The invariant's label, e.g. `G7`.
        label: String,
    },
    /// The Rust catalogue lists an invariant the specification does not.
    Unimplemented {
        /// The invariant's label, e.g. `G7`.
        label: String,
    },
    /// The two copies name different defining sections.
    Sections {
        /// The invariant's label, e.g. `G7`.
        label: String,
        /// What the specification's `Defined in` column says.
        documented: Vec<String>,
        /// What the Rust entry's `spec` field says.
        implemented: Vec<String>,
    },
    /// The two copies name different verifying suites.
    Suites {
        /// The invariant's label, e.g. `G7`.
        label: String,
        /// Named by the specification and not by the Rust catalogue.
        only_documented: Vec<String>,
        /// Named by the Rust catalogue and not by the specification.
        only_implemented: Vec<String>,
    },
}

impl fmt::Display for Mismatch {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Undocumented { label } => {
                write!(
                    f,
                    "{label}: in the spec's table, missing from the Rust catalogue"
                )
            }
            Self::Unimplemented { label } => {
                write!(
                    f,
                    "{label}: in the Rust catalogue, missing from the spec's table"
                )
            }
            Self::Sections {
                label,
                documented,
                implemented,
            } => write!(
                f,
                "{label}: defined in §{} per the spec, §{} per the Rust catalogue",
                documented.join(", §"),
                implemented.join(", §"),
            ),
            Self::Suites {
                label,
                only_documented,
                only_implemented,
            } => {
                write!(f, "{label}: verified-by disagrees")?;
                if !only_documented.is_empty() {
                    write!(f, "; spec-only {only_documented:?}")?;
                }
                if !only_implemented.is_empty() {
                    write!(f, "; catalogue-only {only_implemented:?}")?;
                }
                Ok(())
            }
        }
    }
}

/// The site a catalogue is registered under.
///
/// # Panics
///
/// If `id` names no entry in [`CATALOGUES`]. Callers pass a literal, so a wrong
/// one is a typo to fail on rather than an error to thread through a test.
pub fn site(id: &str) -> &'static CatalogueSite {
    CATALOGUES
        .iter()
        .find(|s| s.id == id)
        .unwrap_or_else(|| panic!("no catalogue site {id:?}; known: {:?}", ids()))
}

fn ids() -> Vec<&'static str> {
    CATALOGUES.iter().map(|s| s.id).collect()
}

/// Read a catalogue's specification copy.
pub fn documented(root: &Path, site: &CatalogueSite) -> io::Result<Vec<Row>> {
    let doc = DOCUMENTS
        .iter()
        .find(|d| d.id == site.doc)
        .ok_or_else(|| io::Error::other(format!("no document {:?}", site.doc)))?;
    let path = root.join(doc.path);
    let text = std::fs::read_to_string(&path)
        .map_err(|e| io::Error::new(e.kind(), format!("reading {}: {e}", path.display())))?;
    let body = section_body(&text, site.section)
        .ok_or_else(|| io::Error::other(format!("{} has no §{}", doc.path, site.section)))?;
    Ok(match site.shape {
        Shape::Table => table_rows(body, site),
        Shape::NumberedList => list_rows(body),
    })
}

/// Compare a specification's catalogue against its Rust copy.
///
/// Both sides are keyed by [`Row::number`]. Suites are compared only when the
/// specification names them ([`Pointers::Prose`] skips that axis).
pub fn compare(site: &CatalogueSite, documented: &[Row], implemented: &[Row]) -> Vec<Mismatch> {
    let label = |n: u8| format!("{}{n}", site.label);
    let mut out = Vec::new();

    for row in implemented {
        if !documented.iter().any(|d| d.number == row.number) {
            out.push(Mismatch::Unimplemented {
                label: label(row.number),
            });
        }
    }
    for doc_row in documented {
        let Some(impl_row) = implemented.iter().find(|i| i.number == doc_row.number) else {
            out.push(Mismatch::Undocumented {
                label: label(doc_row.number),
            });
            continue;
        };
        if doc_row.sections != impl_row.sections {
            out.push(Mismatch::Sections {
                label: label(doc_row.number),
                documented: doc_row.sections.clone(),
                implemented: impl_row.sections.clone(),
            });
        }
        if site.pointers == Pointers::Prose {
            continue;
        }
        let only_documented: Vec<String> = doc_row
            .pointers
            .difference(&impl_row.pointers)
            .cloned()
            .collect();
        let only_implemented: Vec<String> = impl_row
            .pointers
            .difference(&doc_row.pointers)
            .cloned()
            .collect();
        if !only_documented.is_empty() || !only_implemented.is_empty() {
            out.push(Mismatch::Suites {
                label: label(doc_row.number),
                only_documented,
                only_implemented,
            });
        }
    }
    out
}

/// The lines of the section numbered `number`, up to the next heading at the same
/// depth or shallower.
fn section_body<'a>(text: &'a str, number: &str) -> Option<&'a str> {
    let mut start = None;
    let mut depth = 0usize;
    let mut inside = false;
    let mut offset = 0usize;

    for line in text.split_inclusive('\n') {
        let fence = line.trim_start().starts_with("```");
        if fence {
            inside = !inside;
        }
        if !inside
            && !fence
            && let Some((hashes, found)) = heading(line)
        {
            match start {
                None if found == number => {
                    depth = hashes;
                    start = Some(offset + line.len());
                }
                Some(begin) if hashes <= depth => return Some(&text[begin..offset]),
                _ => {}
            }
        }
        offset += line.len();
    }
    start.map(|begin| &text[begin..])
}

/// `### 18.5 Invariant catalogue` → `(3, "18.5")`.
fn heading(line: &str) -> Option<(usize, String)> {
    let hashes = line.len() - line.trim_start_matches('#').len();
    if hashes < 2 {
        return None;
    }
    let rest = line[hashes..].strip_prefix(' ')?;
    let end = rest
        .find(|c: char| !c.is_ascii_digit() && c != '.')
        .unwrap_or(rest.len());
    let number = rest[..end].trim_end_matches('.');
    (!number.is_empty() && number.starts_with(|c: char| c.is_ascii_digit()))
        .then(|| (hashes, number.to_string()))
}

/// Rows of the first table in `body` that has `Invariant` and `Verified by`
/// columns. Columns are found by header name: the harness table carries a fifth
/// `Grain basis` column the others do not.
fn table_rows(body: &str, site: &CatalogueSite) -> Vec<Row> {
    let mut lines = body.lines().skip_while(|l| {
        !(l.starts_with('|') && l.contains("Invariant") && l.contains("Verified by"))
    });
    let Some(header) = lines.next() else {
        return Vec::new();
    };
    let columns: Vec<String> = cells(header).into_iter().map(|c| c.to_string()).collect();
    let column = |name: &str| columns.iter().position(|c| c == name);
    let (Some(defined), Some(verified)) = (column("Defined in"), column("Verified by")) else {
        return Vec::new();
    };

    lines
        .skip_while(|l| l.starts_with("|---"))
        .take_while(|l| l.starts_with('|'))
        .filter_map(|line| {
            let cells = cells(line);
            let number = label_number(cells.first()?, site.label)?;
            Some(Row {
                number,
                sections: section_numbers(cells.get(defined)?),
                pointers: suite_pointers(cells.get(verified)?, site.pointers),
            })
        })
        .collect()
}

/// Rows of a numbered list, each item opening `N. **Property (§a, §b).**`.
fn list_rows(body: &str) -> Vec<Row> {
    body.lines()
        .filter_map(|line| {
            let end = line.find(|c: char| !c.is_ascii_digit())?;
            if end == 0 || !line[end..].starts_with(". ") {
                return None;
            }
            let number: u8 = line[..end].parse().ok()?;
            // Sections live in the bold lead-in; the rest of the item is prose
            // that cites other sections incidentally.
            let lead = line[end..].split("**").nth(1).unwrap_or("");
            Some(Row {
                number,
                sections: section_numbers(lead),
                pointers: BTreeSet::new(),
            })
        })
        .collect()
}

/// The cells of a markdown table row, trimmed, outer pipes dropped.
fn cells(line: &str) -> Vec<&str> {
    line.trim()
        .trim_start_matches('|')
        .trim_end_matches('|')
        .split('|')
        .map(str::trim)
        .collect()
}

/// `**G7**` with label `G` → `7`; `1` with an empty label → `1`.
fn label_number(cell: &str, label: &str) -> Option<u8> {
    let bare = cell.trim_matches(|c| c == '*' || c == '`' || c == ' ');
    bare.strip_prefix(label)?.parse().ok()
}

/// Every `§`-number in `text`, in order: `granary §6, §8` → `["6", "8"]`.
fn section_numbers(text: &str) -> Vec<String> {
    text.match_indices('§')
        .filter_map(|(pos, _)| {
            let after = &text[pos + '§'.len_utf8()..];
            let end = after
                .find(|c: char| !c.is_ascii_digit() && c != '.')
                .unwrap_or(after.len());
            let number = after[..end].trim_end_matches('.');
            (!number.is_empty()).then(|| number.to_string())
        })
        .collect()
}

/// The suite names a `Verified by` cell points at.
///
/// A cell also carries prose — a checker's name, a `(trybuild)` aside, the
/// specific test function inside a file. [`Pointers`] says which of its backticked
/// tokens are the pointers, and the rest are left alone.
fn suite_pointers(cell: &str, kind: Pointers) -> BTreeSet<String> {
    let mut out = BTreeSet::new();
    let mut rest = cell;
    while let Some(open) = rest.find('`') {
        let after = &rest[open + 1..];
        let Some(close) = after.find('`') else { break };
        let token = after[..close].trim();
        rest = &after[close + 1..];
        out.extend(pointer_tokens(token, kind));
    }
    out
}

/// The suite pointers inside a run of text, under one vocabulary.
///
/// Used for both sides: a spec's backticked cell contents and a Rust entry's
/// `Verify` text go through the same filter, so `harness-sandbox/tests/workspace.rs`
/// and `workspace.rs` reduce alike and the prose around them is ignored.
fn pointer_tokens(text: &str, kind: Pointers) -> BTreeSet<String> {
    if kind == Pointers::Prose {
        return BTreeSet::new();
    }
    text.split(|c: char| c.is_whitespace() || matches!(c, ',' | ';' | '(' | ')' | '`'))
        .map(|token| token.trim_matches(|c: char| c == '.' || c == ':' || c == '*'))
        .filter(|token| match kind {
            // A `.rs` file, or a path under a `tests/` directory —
            // `granary/tests/compile_fail` is a trybuild case directory and
            // counts. A slash alone is not enough: the prose around these names
            // says things like "open/release accounting".
            Pointers::Files => token.ends_with(".rs") || token.contains("tests/"),
            Pointers::Functions => {
                token.len() > 3
                    && token.contains('_')
                    && token
                        .chars()
                        .all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || c == '_')
            }
            Pointers::Prose => false,
        })
        .map(stem)
        .filter(|s| !s.is_empty())
        .collect()
}

/// `granary/tests/compile_fail` and `compile_fail.rs` both reduce to
/// `compile_fail`, so a spec naming the trybuild runner matches a catalogue
/// naming its case directory.
fn stem(pointer: &str) -> String {
    pointer
        .trim()
        .trim_matches('`')
        .rsplit('/')
        .next()
        .unwrap_or("")
        .trim_end_matches(".rs")
        .to_string()
}

#[cfg(test)]
mod tests {
    use super::*;

    const GRANARY: CatalogueSite = CatalogueSite {
        id: "granary",
        doc: "granary",
        section: "15",
        label: "G",
        shape: Shape::Table,
        pointers: Pointers::Files,
    };

    #[test]
    fn a_section_body_stops_at_the_next_heading_of_equal_depth() {
        let text = "## 6. One\nbody one\n\n## 7. Two\nbody two\n";
        assert_eq!(section_body(text, "6"), Some("body one\n\n"));
    }

    #[test]
    fn a_section_body_includes_its_subsections() {
        let text = "## 7. One\nintro\n\n### 7.1 Sub\nsub body\n\n## 8. Next\n";
        let body = section_body(text, "7").expect("§7");
        assert!(body.contains("sub body"), "got {body:?}");
    }

    #[test]
    fn a_heading_inside_a_fence_does_not_end_a_section() {
        let text = "## 6. One\n```sh\n## 7. not a heading\n```\nstill six\n\n## 7. Two\n";
        let body = section_body(text, "6").expect("§6");
        assert!(body.contains("still six"), "got {body:?}");
    }

    #[test]
    fn a_table_row_yields_its_number_sections_and_files() {
        let body = "\n| | Invariant | Defined in | Verified by |\n|---|---|---|---|\n\
                    | **G1** | **Single writer.** Only the leader appends. | §6, §8 | \
                    `clustered_grains.rs`, `grains.rs` |\n";
        let rows = table_rows(body, &GRANARY);
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].number, 1);
        assert_eq!(rows[0].sections, ["6", "8"]);
        assert_eq!(
            rows[0].pointers,
            ["clustered_grains".to_string(), "grains".to_string()].into(),
        );
    }

    #[test]
    fn a_verified_by_cell_keeps_files_and_drops_prose() {
        let cell = "`grains.rs`, `grain_swarm.rs` (the `ActivationSingletonPerNode` checker)";
        assert_eq!(
            suite_pointers(cell, Pointers::Files),
            ["grains".to_string(), "grain_swarm".to_string()].into(),
            "a checker's name is prose, not a file",
        );
    }

    #[test]
    fn a_trybuild_runner_and_its_case_directory_are_one_pointer() {
        assert_eq!(stem("compile_fail.rs"), stem("granary/tests/compile_fail"));
    }

    #[test]
    fn a_function_cell_keeps_snake_case_names() {
        let cell = "`a_torn_tail_is_discarded`, `a_record_cut_mid_payload`";
        assert_eq!(
            suite_pointers(cell, Pointers::Functions),
            [
                "a_torn_tail_is_discarded".to_string(),
                "a_record_cut_mid_payload".to_string(),
            ]
            .into(),
        );
    }

    #[test]
    fn columns_are_found_by_name_not_position() {
        let body = "\n| # | Invariant | Defined in | Verified by | Grain basis |\n\
                    |---|---|---|---|---|\n\
                    | H1 | **Fold.** | §6.2 | `sessions.rs` | G2 |\n";
        let site = CatalogueSite {
            label: "H",
            ..GRANARY
        };
        let rows = table_rows(body, &site);
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].sections, ["6.2"]);
        assert_eq!(rows[0].pointers, ["sessions".to_string()].into());
    }

    #[test]
    fn a_numbered_list_takes_sections_from_the_bold_lead_in() {
        let body = "1. **No silent loss (§7.2, §14).** Every `ask` terminates, see §18.3.\n";
        let rows = list_rows(body);
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].number, 1);
        assert_eq!(
            rows[0].sections,
            ["7.2", "14"],
            "§18.3 is prose after the lead-in",
        );
    }

    #[test]
    fn a_row_built_from_a_rust_entry_splits_comma_separated_files() {
        let row = Row::new(
            3,
            "granary §1, §9",
            Pointers::Files,
            ["grains.rs, grain_swarm.rs"],
        );
        assert_eq!(row.sections, ["1", "9"]);
        assert_eq!(
            row.pointers,
            ["grains".to_string(), "grain_swarm".to_string()].into(),
        );
    }

    #[test]
    fn an_en_dash_range_and_a_hyphen_range_agree() {
        assert_eq!(
            section_numbers("§3.2–§3.4"),
            section_numbers("wal §3.2-§3.4")
        );
    }

    #[test]
    fn compare_reports_each_kind_of_disagreement() {
        let documented = vec![
            Row::new(1, "§6", Pointers::Files, ["a.rs"]),
            Row::new(2, "§7", Pointers::Files, ["b.rs"]),
            Row::new(3, "§8", Pointers::Files, ["c.rs"]),
        ];
        let implemented = vec![
            Row::new(1, "§6", Pointers::Files, ["a.rs"]),
            Row::new(2, "§9", Pointers::Files, ["b.rs"]),
            Row::new(4, "§9", Pointers::Files, ["d.rs"]),
        ];
        let found = compare(&GRANARY, &documented, &implemented);
        assert!(found.contains(&Mismatch::Undocumented { label: "G3".into() }));
        assert!(found.contains(&Mismatch::Unimplemented { label: "G4".into() }));
        assert!(
            found
                .iter()
                .any(|m| matches!(m, Mismatch::Sections { label, .. } if label == "G2"))
        );
        assert_eq!(found.len(), 3, "G1 agrees: {found:?}");
    }

    #[test]
    fn compare_skips_suites_when_the_spec_names_none() {
        let site = CatalogueSite {
            shape: Shape::NumberedList,
            pointers: Pointers::Prose,
            ..GRANARY
        };
        let documented = vec![Row::new(1, "§6", Pointers::Prose, Vec::<String>::new())];
        let implemented = vec![Row::new(1, "§6", Pointers::Files, ["conformance_swarm.rs"])];
        assert!(compare(&site, &documented, &implemented).is_empty());
    }
}
