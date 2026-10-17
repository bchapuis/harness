//! The specification side of the invariant catalogues parses.
//!
//! The comparison against each Rust catalogue runs in the owning crate's
//! `conformance_catalogue.rs`, because a Rust catalogue lives in a `tests/` module
//! only its own crate can import. What runs here is the half that does not need
//! it: every site in `CATALOGUES` still names a real section, that section still
//! holds a catalogue, and its rows still parse into numbers and sections.
//! A spec reorganized out from under the table fails here rather than making
//! seven crates' comparisons silently vacuous.

use std::path::Path;
use std::path::PathBuf;

use spec::catalogue::CATALOGUES;
use spec::catalogue::documented;

fn root() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .ancestors()
        .nth(2)
        .expect("crates/spec has a workspace root two levels up")
        .to_path_buf()
}

#[test]
fn every_catalogue_parses_into_rows() {
    for site in CATALOGUES {
        let rows = documented(&root(), site).unwrap_or_else(|e| panic!("{}: {e}", site.id));
        assert!(
            !rows.is_empty(),
            "{}: §{} of the {} spec yielded no catalogue rows",
            site.id,
            site.section,
            site.doc,
        );
        for row in &rows {
            assert!(
                !row.sections.is_empty(),
                "{}: {}{} names no defining section",
                site.id,
                site.label,
                row.number,
            );
        }
    }
}

/// Numbering is per catalogue and runs 1..=n with no gaps: a dropped invariant
/// renumbers the rest rather than leaving its number behind.
#[test]
fn every_catalogue_numbers_its_rows_contiguously() {
    for site in CATALOGUES {
        let rows = documented(&root(), site).unwrap_or_else(|e| panic!("{}: {e}", site.id));
        let mut numbers: Vec<u8> = rows.iter().map(|r| r.number).collect();
        numbers.sort_unstable();
        let expected: Vec<u8> = (1..=u8::try_from(numbers.len()).expect("fits")).collect();
        assert_eq!(numbers, expected, "{}: numbering is not 1..=n", site.id);
    }
}

/// Prints what was parsed. Run with `--ignored --nocapture` when changing the
/// parser; the assertions above are what CI relies on.
#[test]
#[ignore = "diagnostic"]
fn dump() {
    for site in CATALOGUES {
        let rows = documented(&root(), site).unwrap_or_else(|e| panic!("{}: {e}", site.id));
        println!("\n=== {} ({} rows) ===", site.id, rows.len());
        for row in &rows {
            println!(
                "  {}{:<3} §{}",
                site.label,
                row.number,
                row.sections.join(",§"),
            );
        }
    }
}
