//! Catalogue drift test (drive spec §9): the machine-readable D1–D7 catalogue,
//! the live checker set, and the specification's own table must agree — a
//! checker added in code but not recorded in the catalogue (or vice versa), a
//! suite renamed under a pointer, or a row moved in one copy fails the build,
//! the same pattern guarding the core, granary, and harness catalogues.
//!
//! The checker agreement is checked **per (checker, invariant) pair**, as the
//! harness's is: one name carries four D-invariants here, so a name set would be
//! blind to one row quietly dropping its `Verify::Checker` while a sibling row
//! still names the same checker.

mod support;

use std::collections::BTreeSet;

use actor_simulation::Verify;
use actor_simulation::default_invariants;
use drive::testing::checker_coverage;
use drive::testing::drive_invariants;
use spec::catalogue;

use support::catalogue::d_catalogue;

#[test]
fn every_invariant_d1_through_d7_is_present_exactly_once() {
    let mut numbers: Vec<u8> = d_catalogue().iter().map(|e| e.invariant).collect();
    numbers.sort_unstable();
    assert_eq!(
        numbers,
        (1..=7).collect::<Vec<u8>>(),
        "the catalogue must list D1..=D7, each exactly once",
    );
    for e in d_catalogue() {
        assert!(
            !e.verify.is_empty(),
            "D{} has no verification method",
            e.invariant
        );
        assert!(
            !e.spec.is_empty() && !e.property.is_empty(),
            "D{} is missing spec or property text",
            e.invariant
        );
    }
}

/// Every suite the catalogue names still exists under this crate's `tests/`.
#[test]
fn every_file_pointer_references_a_real_file() {
    let tests_dir = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("tests");
    for entry in d_catalogue() {
        for v in entry.verify {
            let files = match v {
                Verify::SimTest(files) | Verify::Differential(files) => files,
                Verify::TestFn(_) => panic!(
                    "D{} points at a test function, and this catalogue points at files",
                    entry.invariant
                ),
                Verify::CompileFail(_)
                | Verify::Checker(_)
                | Verify::CompileTime(_)
                | Verify::Deferred(_) => continue,
            };
            for file in files.split(',').map(str::trim) {
                assert!(
                    tests_dir.join(file).exists(),
                    "D{} points at {file:?}, which does not exist under {}",
                    entry.invariant,
                    tests_dir.display(),
                );
            }
        }
    }
}

/// The pairs the catalogue asserts: every `Verify::Checker` row, with the
/// invariant whose row it is.
fn catalogued_pairs() -> BTreeSet<(&'static str, u8)> {
    d_catalogue()
        .iter()
        .flat_map(|entry| {
            entry.verify.iter().filter_map(|verify| match verify {
                Verify::Checker(name) => Some((*name, entry.invariant)),
                _ => None,
            })
        })
        .collect()
}

#[test]
fn every_checker_covers_exactly_the_invariants_the_catalogue_gives_it() {
    let declared: BTreeSet<(&str, u8)> = checker_coverage().iter().copied().collect();
    assert_eq!(
        declared,
        catalogued_pairs(),
        "a checker's declared coverage (drive::testing::checker_coverage) and the D \
         catalogue's Verify::Checker rows name different (checker, invariant) pairs (§9)"
    );
}

#[test]
fn the_declared_checkers_are_exactly_the_live_ones() {
    let core: BTreeSet<String> = default_invariants()
        .iter()
        .map(|i| i.name().to_string())
        .collect();
    let live: BTreeSet<String> = drive_invariants()
        .iter()
        .map(|i| i.name().to_string())
        .filter(|name| !core.contains(name))
        .collect();
    let declared: BTreeSet<String> = checker_coverage()
        .iter()
        .map(|(name, _)| (*name).to_string())
        .collect();
    assert_eq!(
        live, declared,
        "continuous drive checkers and their declared coverage drifted apart (§9)"
    );
}

/// The table above and the drive spec's §9 copy of it are the same table:
/// numbering, defining sections, and the suites each row points at.
#[test]
fn the_specification_and_this_catalogue_agree() {
    let site = catalogue::site("drive");
    let root = spec::workspace_root(env!("CARGO_MANIFEST_DIR"));
    let documented = catalogue::documented(&root, site).expect("drive spec §9 parses");
    let implemented: Vec<catalogue::Row> = d_catalogue()
        .iter()
        .map(|e| {
            catalogue::Row::new(
                e.invariant,
                e.spec,
                site.pointers,
                e.verify.iter().map(Verify::text),
            )
        })
        .collect();

    let found = catalogue::compare(site, &documented, &implemented);
    assert!(
        found.is_empty(),
        "drive spec §9 and d_catalogue() disagree:\n  {}\n",
        found
            .iter()
            .map(ToString::to_string)
            .collect::<Vec<_>>()
            .join("\n  "),
    );
}
