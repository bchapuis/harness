//! Catalogue drift test (harness spec §11): the machine-readable H1–H8
//! catalogue and the live checker set must agree — a checker added in code
//! but not recorded in the catalogue (or vice versa) fails the build, the
//! same pattern guarding the core and utilities catalogues.
//!
//! The agreement is checked **per (checker, invariant) pair**, not over the two
//! sets of checker names. Two names carry six H-invariants here, so a name set
//! is blind to the drift that actually happens: one entry quietly dropping its
//! `Verify::Checker` while a sibling entry still names the same checker. That is
//! how §11's H6 row came to understate its verification with every test green.

mod support;

use std::collections::BTreeSet;

use actor_simulation::Verify;
use actor_simulation::default_invariants;
use harness::testing::checker_coverage;
use spec_xref::catalogue;

use support::harness_catalogue;

/// Every suite the catalogue names still exists.
///
/// Its pointers are paths relative to `crates/`: the harness's own suites plus the
/// ones it shares with the sandbox provider. A rename that misses this table fails
/// here, as it does in the sibling catalogues.
#[test]
fn every_file_pointer_references_a_real_file() {
    let crates_dir = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .expect("crate dir has a parent");

    for entry in harness_catalogue() {
        for v in entry.verify {
            let files = match v {
                Verify::SimTest(files) | Verify::Differential(files) => files,
                Verify::TestFn(_) => panic!(
                    "H{} points at a test function, and this catalogue points at \
                     files: nothing here scans for a function's definition.",
                    entry.invariant
                ),
                Verify::CompileFail(_)
                | Verify::Checker(_)
                | Verify::CompileTime(_)
                | Verify::Deferred(_) => continue,
            };
            for file in files.split(',').map(str::trim) {
                assert!(
                    crates_dir.join(file).exists(),
                    "H{} points at {file:?}, which does not exist under {}",
                    entry.invariant,
                    crates_dir.display(),
                );
            }
        }
    }
}

use support::harness_invariants;

/// The pairs the catalogue asserts: every `Verify::Checker` row, with the
/// invariant whose row it is.
fn catalogued_pairs() -> BTreeSet<(&'static str, u8)> {
    harness_catalogue()
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
        "a checker's declared coverage (harness::testing::checker_coverage) and the H \
         catalogue's Verify::Checker rows name different (checker, invariant) pairs (§11)"
    );
}

#[test]
fn the_declared_checkers_are_exactly_the_live_ones() {
    // The harness-specific checkers: the harness set minus the core defaults.
    let core: BTreeSet<String> = default_invariants()
        .iter()
        .map(|i| i.name().to_string())
        .collect();
    let live: BTreeSet<String> = harness_invariants()
        .iter()
        .map(|i| i.name().to_string())
        .filter(|name| !core.contains(name))
        .collect();

    // A checker covering nothing is absent from this set, so shipping one
    // without declaring what it carries fails here rather than passing silently.
    let declared: BTreeSet<String> = checker_coverage()
        .iter()
        .map(|(name, _)| (*name).to_string())
        .collect();

    assert_eq!(
        live, declared,
        "continuous harness checkers and their declared coverage drifted apart (§11)"
    );
}

#[test]
fn the_catalogue_covers_the_live_h_invariants() {
    // H2 is retired (§11): the single-writer fence is wholly the grain's (G1),
    // so it has no harness invariant; H3–H8 keep their numbers.
    let numbers: Vec<u8> = harness_catalogue().iter().map(|e| e.invariant).collect();
    assert_eq!(numbers, vec![1, 3, 4, 5, 6, 7, 8]);
    for entry in harness_catalogue() {
        assert!(
            !entry.verify.is_empty(),
            "H{} has no verification method",
            entry.invariant
        );
    }
}

/// The table above and the agentic-harness spec's §11 copy of it are the same
/// table.
///
/// The rest of this file holds the Rust copy internally consistent. This holds it
/// equal to the prose one, which is the copy a reader trusts: an invariant added
/// to one, or a defining section moved in one, is caught here rather than left to
/// be noticed.
///
/// The suite axis is not compared here: the spec's column describes the verification in prose, naming no suite a build could resolve. So this
/// gate covers the numbering and the defining sections, and the "Verified by"
/// column stays a human's to keep true.
#[test]
fn the_specification_and_this_catalogue_agree() {
    let site = catalogue::site("harness");
    let root = spec_xref::workspace_root(env!("CARGO_MANIFEST_DIR"));
    let documented = catalogue::documented(&root, site).expect("agentic-harness spec §11 parses");
    let implemented: Vec<catalogue::Row> = harness_catalogue()
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
        "agentic-harness spec §11 and harness_catalogue() disagree:\n  {}\n",
        found
            .iter()
            .map(ToString::to_string)
            .collect::<Vec<_>>()
            .join("\n  "),
    );
}
