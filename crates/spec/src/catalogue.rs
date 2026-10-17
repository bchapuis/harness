//! The invariant catalogues, read out of the specifications.
//!
//! Each spec carries an invariant catalogue, and each is *also* written down in
//! Rust beside the suite that verifies it: `e_catalogue()`, `w_catalogue()`,
//! `core_catalogue()`, and their siblings. The Rust table is what the per-crate
//! `conformance_catalogue.rs` drift test already guards: it holds the numbering
//! complete and every test it points at real.
//!
//! What neither table guards on its own is that the *two agree*. They are two
//! hand-maintained copies of one fact, and an invariant added to one or a section
//! moved in one leaves the other quietly wrong: the failure the Rust table was
//! introduced to prevent, reappearing one level up.
//!
//! This module reads the specification's copy so a crate's drift test can compare
//! the two. It reads rather than generates because the prose is not derivable: a
//! spec row states the invariant in full, where the Rust `property` field is a
//! one-line summary. Only the load-bearing parts are compared: which invariants
//! exist, and which sections define them.
//!
//! # What each copy owns
//!
//! The specification owns the statement and the defining sections; the Rust copy
//! owns what verifies each row. A spec names no suites, because which test file
//! holds a property is a fact about the code: it moves with the code, and the
//! Rust copy sits where a rename would break it. So the two share exactly the
//! identity and sections axes, and [`compare`] checks those.
//!
//! # The two shapes
//!
//! Six catalogues are markdown tables with `Invariant` and `Defined in`
//! columns. The core catalogue (actor §10) is a numbered list instead
//! ([`Shape`]), for seven sites in all.

use std::fmt;
use std::io;
use std::path::Path;

use crate::DOCUMENTS;
use crate::markdown;

/// How a specification writes its invariant catalogue.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Shape {
    /// A markdown table with `Invariant` and `Defined in` columns, the
    /// invariant's label in the first cell.
    Table,
    /// A numbered list, each item opening `N. **Property (§a, §b).**`.
    NumberedList,
}

/// Where one invariant catalogue lives in the specifications.
#[derive(Debug, Clone, Copy)]
pub struct CatalogueSite {
    /// Short handle, used in failure messages: `entity`, `core`.
    pub id: &'static str,
    /// The [`crate::DocSpec::id`] of the spec that carries it.
    pub doc: &'static str,
    /// The section number the catalogue lives under, e.g. `15`.
    pub section: &'static str,
    /// The letter the labels carry: `E` for `E1`, empty for the core's bare `1.`.
    pub label: &'static str,
    /// How the specification writes it.
    pub shape: Shape,
}

/// Every invariant catalogue in the tree, and where its specification copy lives.
///
/// The Rust copy is *not* named here: it lives in a `tests/` module that only its
/// own crate can import, which is why the comparison runs inside each crate's
/// `conformance_catalogue.rs` rather than centrally.
pub const CATALOGUES: &[CatalogueSite] = &[
    // A numbered list rather than a table.
    CatalogueSite {
        id: "core",
        doc: "actor",
        section: "10",
        label: "",
        shape: Shape::NumberedList,
    },
    CatalogueSite {
        id: "utilities",
        doc: "utilities",
        section: "4",
        label: "U",
        shape: Shape::Table,
    },
    CatalogueSite {
        id: "entity",
        doc: "entity",
        section: "12",
        label: "E",
        shape: Shape::Table,
    },
    CatalogueSite {
        id: "workflow",
        doc: "workflow",
        section: "8",
        label: "W",
        shape: Shape::Table,
    },
    CatalogueSite {
        id: "agent",
        doc: "agent",
        section: "10",
        label: "A",
        shape: Shape::Table,
    },
    CatalogueSite {
        id: "sandbox",
        doc: "sandbox",
        section: "6",
        label: "S",
        shape: Shape::Table,
    },
    CatalogueSite {
        id: "machine",
        doc: "machine",
        section: "6",
        label: "M",
        shape: Shape::Table,
    },
];

/// One catalogue row, reduced to the parts both copies state.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Row {
    /// The number within its catalogue: `7` for `E7`.
    pub number: u8,
    /// The sections that define it, as written: `["6", "8"]` for `§6, §8`.
    pub sections: Vec<String>,
}

impl Row {
    /// Build the implementation's side of a row from a Rust catalogue entry.
    ///
    /// `spec` is the entry's section list in any of the forms the tables use
    /// (`§6, §8`, `entity §6, §8`, `§3.2-§3.4`), only the numbers are kept.
    pub fn new(number: u8, spec: &str) -> Self {
        Self {
            number,
            sections: section_numbers(spec),
        }
    }
}

/// A disagreement between a specification's catalogue and its Rust copy.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Mismatch {
    /// The specification lists an invariant the Rust catalogue does not.
    Undocumented {
        /// The invariant's label, e.g. `E7`.
        label: String,
    },
    /// The Rust catalogue lists an invariant the specification does not.
    Unimplemented {
        /// The invariant's label, e.g. `E7`.
        label: String,
    },
    /// The two copies name different defining sections.
    Sections {
        /// The invariant's label, e.g. `E7`.
        label: String,
        /// What the specification's `Defined in` column says.
        documented: Vec<String>,
        /// What the Rust entry's `spec` field says.
        implemented: Vec<String>,
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
/// Both sides are keyed by [`Row::number`].
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
    }
    out
}

/// The lines of the section numbered `number`, up to the next heading at the same
/// depth or shallower.
fn section_body<'a>(text: &'a str, number: &str) -> Option<&'a str> {
    let mut start = None;
    let mut depth = 0usize;

    for line in markdown::lines(text) {
        if line.fenced() {
            continue;
        }
        let Some((hashes, found)) = markdown::heading(line.text) else {
            continue;
        };
        match start {
            None if found == number => {
                depth = hashes;
                start = Some(line.end);
            }
            Some(begin) if hashes <= depth => return Some(&text[begin..line.start]),
            _ => {}
        }
    }
    start.map(|begin| &text[begin..])
}

/// Rows of the first table in `body` that has `Invariant` and `Defined in`
/// columns. Columns are found by header name, so a table may carry others.
fn table_rows(body: &str, site: &CatalogueSite) -> Vec<Row> {
    let mut lines = body.lines().skip_while(|l| {
        !(l.starts_with('|') && l.contains("Invariant") && l.contains("Defined in"))
    });
    let Some(header) = lines.next() else {
        return Vec::new();
    };
    let columns: Vec<String> = cells(header).into_iter().map(|c| c.to_string()).collect();
    let column = |name: &str| columns.iter().position(|c| c == name);
    let Some(defined) = column("Defined in") else {
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
            })
        })
        .collect()
}

/// Rows of a numbered list, each item opening `N. **Property (§a, §b).**`.
fn list_rows(body: &str) -> Vec<Row> {
    body.lines()
        .filter_map(|line| {
            let (number, item) = markdown::list_item(line)?;
            let number = u8::try_from(number).ok()?;
            // Sections live in the bold lead-in; the rest of the item is prose
            // that cites other sections incidentally.
            let lead = item.split("**").nth(1).unwrap_or("");
            Some(Row {
                number,
                sections: section_numbers(lead),
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

/// `**E7**` with label `E` → `7`; `1` with an empty label → `1`.
fn label_number(cell: &str, label: &str) -> Option<u8> {
    let bare = cell.trim_matches(|c| c == '*' || c == '`' || c == ' ');
    bare.strip_prefix(label)?.parse().ok()
}

/// Every `§`-number in `text`, in order: `entity §6, §8` → `["6", "8"]`.
fn section_numbers(text: &str) -> Vec<String> {
    text.match_indices('§')
        .filter_map(|(pos, _)| {
            let (number, _) = markdown::dotted_number(&text[pos + '§'.len_utf8()..]);
            (!number.is_empty()).then(|| number.to_string())
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    const ENTITY: CatalogueSite = CatalogueSite {
        id: "entity",
        doc: "entity",
        section: "15",
        label: "E",
        shape: Shape::Table,
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
    fn a_table_row_yields_its_number_and_sections() {
        let body = "\n| | Invariant | Defined in |\n|---|---|---|\n\
                    | **E1** | **Single writer.** Only the leader appends. | §6, §8 |\n";
        let rows = table_rows(body, &ENTITY);
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].number, 1);
        assert_eq!(rows[0].sections, ["6", "8"]);
    }

    #[test]
    fn columns_are_found_by_name_not_position() {
        let body = "\n| # | Invariant | Entity basis | Defined in |\n\
                    |---|---|---|---|\n\
                    | A1 | **Fold.** | E2 | §6.2 |\n";
        let site = CatalogueSite {
            label: "A",
            ..ENTITY
        };
        let rows = table_rows(body, &site);
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].sections, ["6.2"]);
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
    fn a_row_built_from_a_rust_entry_keeps_only_section_numbers() {
        assert_eq!(Row::new(3, "entity §1, §9").sections, ["1", "9"]);
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
        let documented = vec![Row::new(1, "§6"), Row::new(2, "§7"), Row::new(3, "§8")];
        let implemented = vec![Row::new(1, "§6"), Row::new(2, "§9"), Row::new(4, "§9")];
        let found = compare(&ENTITY, &documented, &implemented);
        assert!(found.contains(&Mismatch::Undocumented { label: "E3".into() }));
        assert!(found.contains(&Mismatch::Unimplemented { label: "E4".into() }));
        assert!(
            found
                .iter()
                .any(|m| matches!(m, Mismatch::Sections { label, .. } if label == "E2"))
        );
        assert_eq!(found.len(), 3, "E1 agrees: {found:?}");
    }
}
