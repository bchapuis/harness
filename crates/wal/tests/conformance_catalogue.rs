//! Catalogue drift test: the machine-readable L1-L4 table stays complete, every
//! test it names still exists, and it agrees with the invariant table in the
//! crate docs, which state the contract.
//!
//! # Why this one is self-contained
//!
//! Its siblings share `actor_simulation::{CatalogueEntry, Verify}`. Nothing
//! forbids that here (`actor-simulation` does not depend on this crate, so the
//! dev-dependency would close no cycle), but it would cost more than it gives.
//! `wal` is a leaf: it depends on `compat` and third-party crates only, and a
//! dev-dependency on the simulation crate would build the actor runtime and the
//! cluster stack beneath it to run this crate's tests, for a struct of four
//! fields. And the shared `Verify` is a vocabulary for pointing at suites and
//! checkers, while every row here names test functions only. So the shape is
//! mirrored here in a dozen lines rather than imported.
//!
//! # Why this one checks names, not files
//!
//! The other catalogues point at test *files*, because their suites are files.
//! Nearly all of wal's verification is `#[test]` functions inside `src/lib.rs`, so
//! a file-existence check would assert only that `lib.rs` still exists: true, and
//! worth nothing. This scans for each named function instead, which is the check
//! that catches the failure that matters: a renamed test leaving a row pointing at
//! nothing.

use std::path::Path;

/// One row of the L-catalogue: the invariant number, the crate-doc sections
/// defining it, a one-line property, and the test functions verifying it.
struct CatalogueEntry {
    invariant: u8,
    /// Headings of the crate root's `# Contract` section, as written there.
    defined_in: &'static [&'static str],
    property: &'static str,
    tests: &'static [&'static str],
}

const L_CATALOGUE: &[CatalogueEntry] = &[
    CatalogueEntry {
        invariant: 1,
        defined_in: &["Opening and recovery"],
        property: "Prefix recovery: open returns exactly the maximal prefix of valid frames; the first incomplete, oversized, checksum-failing, or unparsable frame and everything after it is discarded, and the file is truncated to the valid length before any append",
        tests: &[
            "a_torn_tail_is_discarded_and_appends_continue",
            "a_record_cut_mid_payload_is_discarded",
            "a_corrupted_checksum_ends_the_valid_prefix",
            "every_truncation_recovers_a_prefix_and_stays_recovered",
            "every_single_byte_corruption_drops_that_record_and_the_rest",
            "a_length_header_claiming_more_than_the_file_holds_is_refused",
        ],
    },
    CatalogueEntry {
        invariant: 2,
        defined_in: &["Writing"],
        property: "Acknowledged-record durability: a record acknowledged by a returned append/append_batch/rewrite is recovered byte-identically across a reopen, unless a later truncate or rewrite removes it",
        tests: &[
            "records_round_trip_across_a_reopen",
            "truncate_drops_a_conflicting_suffix",
            "rewrite_replaces_the_whole_file_and_reopens",
            "every_truncation_recovers_a_prefix_and_stays_recovered",
        ],
    },
    CatalogueEntry {
        invariant: 3,
        defined_in: &["Writing", "Whole-file replacement and sidecars"],
        property: "Atomic whole-file replacement: rewrite and atomic_replace leave, after any crash, either the whole prior file or the whole new file, never a torn intermediate; concurrent atomic_replace calls on one name all succeed, and the file that lands is one caller's whole bytes",
        tests: &[
            "rewrite_replaces_the_whole_file_and_reopens",
            "atomic_replace_round_trips_a_sidecar",
            "concurrent_replaces_of_one_name_all_succeed_and_none_tears",
            "rewrite_restamps_the_replacement",
            "a_scratch_file_left_by_a_crash_mid_rewrite_leaves_the_prior_log_whole",
        ],
    },
    CatalogueEntry {
        invariant: 4,
        defined_in: &["The record bound"],
        property: "Write/recovery bound agreement: max_record bounds a frame's payload identically at both ends, so a payload recovery would reject for size cannot be written; the write panics instead of acknowledging a record the next open would silently drop",
        tests: &["appending_a_record_past_the_limit_panics_instead_of_losing_it"],
    },
];

/// Every source file in this crate that may define a `#[test]`.
fn crate_sources() -> String {
    let root = Path::new(env!("CARGO_MANIFEST_DIR"));
    let mut all = String::new();
    for rel in ["src/lib.rs", "tests/crash_points.rs"] {
        let path = root.join(rel);
        all.push_str(
            &std::fs::read_to_string(&path)
                .unwrap_or_else(|e| panic!("reading {}: {e}", path.display())),
        );
        all.push('\n');
    }
    all
}

#[test]
fn every_invariant_l1_through_l4_is_present_exactly_once() {
    let mut numbers: Vec<u8> = L_CATALOGUE.iter().map(|e| e.invariant).collect();
    numbers.sort_unstable();
    assert_eq!(
        numbers,
        (1..=4).collect::<Vec<u8>>(),
        "the catalogue must list L1..=L4, each exactly once",
    );
}

#[test]
fn every_entry_has_a_section_property_and_a_verification_method() {
    for e in L_CATALOGUE {
        assert!(
            !e.tests.is_empty(),
            "L{} names no verifying test",
            e.invariant
        );
        assert!(
            !e.defined_in.is_empty() && !e.property.is_empty(),
            "L{} is missing its defining section or property text",
            e.invariant
        );
    }
}

/// The gate: every test the catalogue names is still defined somewhere in the
/// crate. A rename that misses this table fails the build.
#[test]
fn every_named_test_exists() {
    let sources = crate_sources();
    for e in L_CATALOGUE {
        for name in e.tests {
            assert!(
                sources.contains(&format!("fn {name}(")),
                "L{} names test {name:?}, which is not defined in this crate; \
                 a rename left this row pointing at nothing",
                e.invariant,
            );
        }
    }
}

/// The reverse direction, scoped to the exhaustive suite: every crash-point test
/// is claimed by some invariant.
///
/// Only `crash_points.rs` is held to this. `src/lib.rs` legitimately carries tests
/// that pin no numbered invariant (the header re-stamping rule, the
/// `max_record`/magic aliasing assert), and requiring an L-number for each
/// would push those numbers into the contract to satisfy a test, which is
/// backwards. The crash-point suite has no such cases: it exists solely
/// to decide L1 and L2.
#[test]
fn every_crash_point_test_is_claimed_by_an_invariant() {
    let claimed: Vec<&str> = L_CATALOGUE
        .iter()
        .flat_map(|e| e.tests.iter().copied())
        .collect();
    let source = std::fs::read_to_string(
        Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/crash_points.rs"),
    )
    .expect("crash_points.rs is readable");

    for line in source.lines() {
        let line = line.trim();
        let Some(rest) = line.strip_prefix("fn ") else {
            continue;
        };
        let Some(name) = rest.split('(').next() else {
            continue;
        };
        // Helpers (`entries`, `written`, `recover`) take arguments; the tests take none.
        if !rest.starts_with(&format!("{name}()")) {
            continue;
        }
        assert!(
            claimed.contains(&name),
            "crash_points.rs defines test {name:?}, which no L-invariant claims; \
             either record it in the catalogue or say why it verifies nothing",
        );
    }
}

/// The table above and the crate docs' invariant table are the same table.
///
/// The gates above hold this copy internally consistent and its names real. This
/// holds it equal to the one in `src/lib.rs`, which is the copy a reader trusts:
/// each row's number appears there exactly once, and each section it names is a
/// heading of the crate docs.
#[test]
fn the_crate_docs_and_this_catalogue_agree() {
    let path = Path::new(env!("CARGO_MANIFEST_DIR")).join("src/lib.rs");
    let source = std::fs::read_to_string(&path).expect("src/lib.rs is readable");
    let docs: Vec<&str> = source
        .lines()
        .take_while(|line| line.starts_with("//!"))
        .collect();

    let rows: Vec<&str> = docs
        .iter()
        .filter_map(|line| line.strip_prefix("//! | L"))
        .collect();
    assert_eq!(
        rows.len(),
        L_CATALOGUE.len(),
        "the crate docs' invariant table and L_CATALOGUE list different rows",
    );
    for e in L_CATALOGUE {
        let label = format!("{} |", e.invariant);
        assert_eq!(
            rows.iter().filter(|row| row.starts_with(&label)).count(),
            1,
            "L{} must appear exactly once in the crate docs' invariant table",
            e.invariant,
        );
        for section in e.defined_in {
            assert!(
                docs.contains(&format!("//! ## {section}").as_str()),
                "L{} is defined in {section:?}, which is not a heading of the crate docs",
                e.invariant,
            );
        }
    }
}
