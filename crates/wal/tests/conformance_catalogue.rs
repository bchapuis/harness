//! Catalogue drift test (wal spec §7): the machine-readable W1–W5 table stays
//! complete, and every test it names still exists.
//!
//! # Why this one is self-contained
//!
//! Its siblings share `actor_simulation::{CatalogueEntry, Verify}`. This crate
//! cannot: `wal` "sits below its callers and depends on none of them" (spec §1),
//! and `actor-simulation` depends on `actor-cluster`, which depends on `wal` for
//! its Raft log. A dev-dependency on the shared types would close that loop —
//! Cargo tolerates a dev-dependency cycle, but the layering claim in §1 is load
//! bearing, and a crate that quietly depends on its own consumers is exactly what
//! it exists to rule out. So the shape is mirrored here in a dozen lines rather
//! than imported.
//!
//! # Why this one checks names, not files
//!
//! The other catalogues point at test *files*, because their suites are files.
//! Nearly all of wal's verification is `#[test]` functions inside `src/lib.rs`, so
//! a file-existence check would assert only that `lib.rs` still exists — true, and
//! worth nothing. This scans for each named function instead, which is the check
//! that actually catches the blob-store failure mode: a renamed test leaving the
//! spec's "Verified by" column pointing at nothing.

use std::path::Path;

use spec::catalogue;

/// One row of the W-catalogue: the invariant number, the spec sections defining
/// it, a one-line property, and the test functions verifying it.
struct CatalogueEntry {
    invariant: u8,
    spec: &'static str,
    property: &'static str,
    tests: &'static [&'static str],
}

const W_CATALOGUE: &[CatalogueEntry] = &[
    CatalogueEntry {
        invariant: 1,
        spec: "wal §3.1",
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
        spec: "wal §3.2-§3.4",
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
        spec: "wal §3.4, §5",
        property: "Atomic whole-file replacement: rewrite and atomic_replace leave, after any crash, either the whole prior file or the whole new file, never a torn intermediate; concurrent atomic_replace calls on one name all succeed, and the file that lands is one caller's whole bytes",
        tests: &[
            "rewrite_replaces_the_whole_file_and_reopens",
            "atomic_replace_round_trips_a_sidecar",
            "concurrent_replaces_of_one_name_all_succeed_and_none_tears",
            "rewrite_restamps_the_replacement",
        ],
    },
    CatalogueEntry {
        invariant: 4,
        spec: "wal §4",
        property: "Write/recovery bound agreement: max_record bounds a frame's payload identically at both ends, so a payload recovery would reject for size cannot be written — the write panics instead of acknowledging a record the next open would silently drop",
        tests: &["appending_a_record_past_the_limit_panics_instead_of_losing_it"],
    },
    CatalogueEntry {
        invariant: 5,
        spec: "wal §2 rule 2, §2.1 rule 4",
        property: "A file is read at the digest it records: a log opens, scans, and continues to be appended with the checksum kind its own header names; a kind the build cannot compute is refused by name, and only rewrite restamps one",
        tests: &[
            "a_log_at_an_older_digest_still_opens_and_keeps_its_digest",
            "a_foreign_checksum_kind_is_refused_by_name",
        ],
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
fn every_invariant_w1_through_w5_is_present_exactly_once() {
    let mut numbers: Vec<u8> = W_CATALOGUE.iter().map(|e| e.invariant).collect();
    numbers.sort_unstable();
    assert_eq!(
        numbers,
        (1..=5).collect::<Vec<u8>>(),
        "the catalogue must list W1..=W5, each exactly once",
    );
}

#[test]
fn every_entry_has_spec_property_and_a_verification_method() {
    for e in W_CATALOGUE {
        assert!(
            !e.tests.is_empty(),
            "W{} names no verifying test",
            e.invariant
        );
        assert!(
            !e.spec.is_empty() && !e.property.is_empty(),
            "W{} is missing spec or property text",
            e.invariant
        );
    }
}

/// The gate: every test the catalogue names is still defined somewhere in the
/// crate. A rename that misses this table fails the build.
#[test]
fn every_named_test_exists() {
    let sources = crate_sources();
    for e in W_CATALOGUE {
        for name in e.tests {
            assert!(
                sources.contains(&format!("fn {name}(")),
                "W{} names test {name:?}, which is not defined in this crate — \
                 a rename left the spec's \"Verified by\" column pointing at nothing",
                e.invariant,
            );
        }
    }
}

/// The reverse direction, scoped to the exhaustive suite: every crash-point test
/// is claimed by some invariant.
///
/// Only `crash_points.rs` is held to this. `src/lib.rs` legitimately carries tests
/// that pin no numbered invariant — the frozen sidecar digest, the header
/// re-stamping rule, the `max_record`/magic aliasing assert — and requiring a
/// W-number for each would push those numbers into the spec to satisfy a test,
/// which is backwards. The crash-point suite has no such cases: it exists solely
/// to decide W1 and W2.
#[test]
fn every_crash_point_test_is_claimed_by_an_invariant() {
    let claimed: Vec<&str> = W_CATALOGUE
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
            "crash_points.rs defines test {name:?}, which no W-invariant claims — \
             either record it in the catalogue or say why it verifies nothing",
        );
    }
}

/// The table above and the wal spec's §7 copy of it are the same table.
///
/// The gates above hold this copy internally consistent and its names real. This
/// holds it equal to the prose one, which is the copy a reader trusts.
///
/// `spec` is safe to take here where `actor-simulation` was not (see the
/// header): it reads `docs/` and depends on nothing, so it adds no layer beneath
/// this crate and closes no cycle. Both copies name test *functions*, which makes
/// this the one catalogue whose "Verified by" column compares as written.
#[test]
fn the_specification_and_this_catalogue_agree() {
    let site = catalogue::site("wal");
    let root = spec::workspace_root(env!("CARGO_MANIFEST_DIR"));
    let documented = catalogue::documented(&root, site).expect("wal spec §7 parses");
    let implemented: Vec<catalogue::Row> = W_CATALOGUE
        .iter()
        .map(|e| catalogue::Row::new(e.invariant, e.spec, site.pointers, e.tests.iter().copied()))
        .collect();

    let found = catalogue::compare(site, &documented, &implemented);
    assert!(
        found.is_empty(),
        "wal spec §7 and W_CATALOGUE disagree:\n  {}\n",
        found
            .iter()
            .map(ToString::to_string)
            .collect::<Vec<_>>()
            .join("\n  "),
    );
}
