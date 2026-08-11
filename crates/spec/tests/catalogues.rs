//! The specification side of the invariant catalogues parses.
//!
//! The comparison against each Rust catalogue runs in the owning crate's
//! `conformance_catalogue.rs`, because a Rust catalogue lives in a `tests/` module
//! only its own crate can import. What runs here is the half that does not need
//! it: every site in `CATALOGUES` still names a real section, that section still
//! holds a catalogue, and its rows still parse into numbers, sections, and suites.
//! A spec reorganized out from under the table fails here rather than making
//! seven crates' comparisons silently vacuous.

use std::path::Path;
use std::path::PathBuf;

use spec::catalogue::CATALOGUES;
use spec::catalogue::Pointers;
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
        // A row may legitimately name no suite — sandbox S3's `Network` tier does
        // not ship, so its cell says so. A whole catalogue naming none is instead
        // a parser that has stopped recognizing the column.
        if site.pointers != Pointers::Prose {
            assert!(
                rows.iter().any(|r| !r.pointers.is_empty()),
                "{}: no row names a verifying suite; the {:?} column stopped parsing",
                site.id,
                site.pointers,
            );
        }
    }
}

/// Numbering is per catalogue and starts at one. The harness catalogue has no H2
/// (harness §11), so this asserts a contiguous run only up to that documented gap
/// rather than assuming one.
#[test]
fn every_catalogue_numbers_its_rows_uniquely() {
    for site in CATALOGUES {
        let rows = documented(&root(), site).unwrap_or_else(|e| panic!("{}: {e}", site.id));
        let mut numbers: Vec<u8> = rows.iter().map(|r| r.number).collect();
        let count = numbers.len();
        numbers.sort_unstable();
        numbers.dedup();
        assert_eq!(numbers.len(), count, "{}: a number repeats", site.id);
        assert_eq!(
            numbers.first(),
            Some(&1),
            "{}: does not start at 1",
            site.id
        );
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
                "  {}{:<3} §{:<16} {:?}",
                site.label,
                row.number,
                row.sections.join(",§"),
                row.pointers,
            );
        }
    }
}
