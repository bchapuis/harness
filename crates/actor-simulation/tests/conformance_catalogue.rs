//! Conformance: the §18.5 invariant catalogue is the single source of truth and
//! stays consistent with the live checker set (spec §17, §18.5, §18.6).
//!
//! This is the spec↔code drift gate. It fails the build if an invariant number
//! is missing or duplicated, if a catalogue `Checker(name)` is not actually
//! present in `default_invariants()`, if a live checker is not recorded in the
//! catalogue, or if a `SimTest`/`Differential`/`CompileFail` file pointer names
//! a test file that no longer exists. Keeping it green is what makes the §17
//! "Verified by" column mechanically true rather than just documented.

use std::collections::BTreeSet;
use std::path::Path;

use actor_simulation::Catalogue;
use actor_simulation::CheckerCoverage;
use actor_simulation::Verify;
use actor_simulation::checker_coverage;
use actor_simulation::core_catalogue;
use actor_simulation::default_invariants;
use actor_simulation::utilities_catalogue;
use spec::catalogue;

#[test]
fn every_invariant_1_through_22_is_present_exactly_once() {
    let mut numbers: Vec<u8> = core_catalogue().iter().map(|e| e.invariant).collect();
    numbers.sort_unstable();
    assert_eq!(
        numbers,
        (1..=22).collect::<Vec<u8>>(),
        "catalogue must list invariants #1..=#22, each exactly once"
    );
}

#[test]
fn every_entry_has_spec_property_and_a_verification_method() {
    for e in core_catalogue() {
        assert!(
            !e.verify.is_empty(),
            "invariant #{} has no verification method",
            e.invariant
        );
        assert!(
            !e.spec.is_empty() && !e.property.is_empty(),
            "invariant #{} is missing spec or property text",
            e.invariant
        );
    }
}

/// The utilities catalogue (utilities spec §6) is held to the same drift
/// discipline as the core table: U-numbers contiguous from U1, every entry
/// fully described and verified somehow.
#[test]
fn every_utilities_invariant_is_present_exactly_once_and_described() {
    let mut numbers: Vec<u8> = utilities_catalogue().iter().map(|e| e.invariant).collect();
    numbers.sort_unstable();
    let expected: Vec<u8> = (1..=numbers.len() as u8).collect();
    assert_eq!(
        numbers,
        expected,
        "utilities catalogue must list U1..=U{}, each exactly once",
        expected.len()
    );
    for e in utilities_catalogue() {
        assert!(
            !e.verify.is_empty() && !e.spec.is_empty() && !e.property.is_empty(),
            "utilities invariant U{} is missing spec, property, or verification",
            e.invariant
        );
    }
}

/// The pairs the catalogues assert: every `Verify::Checker` row, tagged with
/// the table it sits in and the invariant whose row it is.
fn catalogued_coverage() -> BTreeSet<CheckerCoverage> {
    let mut pairs = BTreeSet::new();
    for (catalogue, entries) in [
        (Catalogue::Core, core_catalogue()),
        (Catalogue::Utilities, utilities_catalogue()),
    ] {
        for entry in entries {
            for verify in entry.verify {
                if let Verify::Checker(name) = verify {
                    pairs.insert(CheckerCoverage {
                        checker: name,
                        catalogue,
                        invariant: entry.invariant,
                    });
                }
            }
        }
    }
    pairs
}

/// Checker names actually wired into `default_invariants()`.
fn live_checker_names() -> BTreeSet<&'static str> {
    default_invariants().iter().map(|i| i.name()).collect()
}

/// The pairing, both ways and **per invariant** — not over the two sets of
/// checker *names*, which cannot see one entry dropping its `Verify::Checker`
/// while a sibling entry still names the same checker. Today each core checker
/// carries exactly one invariant, so the two comparisons happen to agree; the
/// pair check is what keeps that a fact rather than an assumption the moment a
/// checker earns a second row.
#[test]
fn every_checker_covers_exactly_the_invariants_the_catalogues_give_it() {
    let declared: BTreeSet<CheckerCoverage> = checker_coverage().iter().copied().collect();
    assert_eq!(
        declared,
        catalogued_coverage(),
        "a checker's declared coverage (actor_simulation::checker_coverage) and the \
         catalogues' Verify::Checker rows name different (checker, invariant) pairs (§18.5)"
    );
}

#[test]
fn the_declared_checkers_are_exactly_the_live_ones() {
    // A checker covering nothing is absent from this set, so shipping one
    // without declaring what it carries fails here rather than passing silently.
    let declared: BTreeSet<&'static str> = checker_coverage().iter().map(|c| c.checker).collect();
    assert_eq!(
        live_checker_names(),
        declared,
        "default_invariants() and the declared checker coverage drifted apart (§18.5)"
    );
}

/// Every `SimTest`/`Differential`/`CompileFail` file pointer must name a file
/// that actually exists — otherwise a renamed or deleted conformance test drifts
/// out from under the catalogue unnoticed, and the §17 "Verified by" column stops
/// being mechanically true. `SimTest`/`Differential` pointers are comma-separated
/// `*.rs` files under this crate's `tests/` directory; a `CompileFail` pointer is
/// a path relative to the `crates/` directory (e.g. `actor-core/tests/compile_fail`).
#[test]
fn every_file_pointer_references_a_real_file() {
    let tests_dir = Path::new(env!("CARGO_MANIFEST_DIR")).join("tests");
    let crates_dir = Path::new(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .expect("crate dir has a parent");

    for e in core_catalogue().iter().chain(utilities_catalogue()) {
        for v in e.verify {
            match v {
                Verify::SimTest(files) | Verify::Differential(files) => {
                    for file in files.split(',').map(str::trim) {
                        assert!(
                            tests_dir.join(file).exists(),
                            "invariant #{} points at test file {file:?}, \
                             which does not exist under {}",
                            e.invariant,
                            tests_dir.display(),
                        );
                    }
                }
                Verify::CompileFail(path) => {
                    assert!(
                        crates_dir.join(path).exists(),
                        "invariant #{} points at compile-fail path {path:?}, \
                         which does not exist under {}",
                        e.invariant,
                        crates_dir.display(),
                    );
                }
                Verify::TestFn(_) => panic!(
                    "invariant #{} points at a test function, and this catalogue points at \
                     files: nothing here scans for a function's definition. Give it a \
                     name gate of its own first, as blob-store and machine-grain have.",
                    e.invariant
                ),
                Verify::Checker(_) | Verify::CompileTime(_) | Verify::Deferred(_) => {}
            }
        }
    }
}

#[test]
fn invariant_20_is_verified_by_a_compile_fail_test() {
    let twenty = core_catalogue()
        .iter()
        .find(|e| e.invariant == 20)
        .expect("invariant #20 must be catalogued");
    assert!(
        twenty
            .verify
            .iter()
            .any(|v| matches!(v, Verify::CompileFail(_))),
        "invariant #20 (type-safety) must be verified by a compile-fail test"
    );
}

/// The table above and the actor spec's §18.5 copy of it are the same
/// table.
///
/// The rest of this file holds the Rust copy internally consistent. This holds it
/// equal to the prose one, which is the copy a reader trusts: an invariant added
/// to one, or a defining section moved in one, is caught here rather than left to
/// be noticed.
///
/// The suite axis is not compared here: §18.5 is a numbered list that names no suites at all; §18.6 describes the layering instead. So this
/// gate covers the numbering and the defining sections, and the "Verified by"
/// column stays a human's to keep true.
#[test]
fn the_specification_and_the_core_catalogue_agree() {
    let site = catalogue::site("core");
    let root = spec::workspace_root(env!("CARGO_MANIFEST_DIR"));
    let documented = catalogue::documented(&root, site).expect("actor spec §18.5 parses");
    let implemented: Vec<catalogue::Row> = core_catalogue()
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
        "actor spec §18.5 and core_catalogue() disagree:\n  {}\n",
        found
            .iter()
            .map(ToString::to_string)
            .collect::<Vec<_>>()
            .join("\n  "),
    );
}

/// The table above and the cluster-utilities spec's §6 copy of it are the same
/// table.
///
/// The rest of this file holds the Rust copy internally consistent. This holds it
/// equal to the prose one, which is the copy a reader trusts: an invariant added
/// to one, or a defining section moved in one, is caught here rather than left to
/// be noticed.
#[test]
fn the_specification_and_the_utilities_catalogue_agree() {
    let site = catalogue::site("utilities");
    let root = spec::workspace_root(env!("CARGO_MANIFEST_DIR"));
    let documented = catalogue::documented(&root, site).expect("cluster-utilities spec §6 parses");
    let implemented: Vec<catalogue::Row> = utilities_catalogue()
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
        "cluster-utilities spec §6 and utilities_catalogue() disagree:\n  {}\n",
        found
            .iter()
            .map(ToString::to_string)
            .collect::<Vec<_>>()
            .join("\n  "),
    );
}
