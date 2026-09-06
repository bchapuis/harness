//! Catalogue drift test (entity spec §15): the machine-readable E1–E21 table
//! stays complete, and every file it points at still exists.
//!
//! This is the spec↔code drift gate for entity, the same pattern that guards the
//! core, utilities, agent, and sandbox catalogues. It fails the build if a
//! E-number is missing or duplicated, if an entry claims no verification method,
//! or if a `SimTest`/`CompileFail` pointer names a file that no longer exists —
//! which is what makes §15's "Verified by" column mechanically true rather than
//! merely written down.

mod support;

use std::path::Path;

use actor_simulation::Verify;
use spec::catalogue;

use support::catalogue::e_catalogue;

/// The table above and the spec's §15 copy of it are the same table.
///
/// The rest of this file holds the Rust copy internally consistent. This holds it
/// equal to the prose one, which is the copy a reader trusts: an invariant added
/// to one, a defining section moved in one, or a suite renamed in one is caught
/// here rather than left to be noticed.
#[test]
fn the_specification_and_this_catalogue_agree() {
    let site = catalogue::site("entity");
    let root = spec::workspace_root(env!("CARGO_MANIFEST_DIR"));
    let documented = catalogue::documented(&root, site).expect("entity §15 parses");
    let implemented: Vec<catalogue::Row> = e_catalogue()
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
        "entity spec §15 and e_catalogue() disagree:\n  {}\n",
        found
            .iter()
            .map(ToString::to_string)
            .collect::<Vec<_>>()
            .join("\n  "),
    );
}

#[test]
fn every_invariant_e1_through_e21_is_present_exactly_once() {
    let mut numbers: Vec<u8> = e_catalogue().iter().map(|e| e.invariant).collect();
    numbers.sort_unstable();
    assert_eq!(
        numbers,
        (1..=21).collect::<Vec<u8>>(),
        "the catalogue must list E1..=E21, each exactly once",
    );
}

#[test]
fn every_entry_has_spec_property_and_a_verification_method() {
    for e in e_catalogue() {
        assert!(
            !e.verify.is_empty(),
            "E{} has no verification method",
            e.invariant
        );
        assert!(
            !e.spec.is_empty() && !e.property.is_empty(),
            "E{} is missing spec or property text",
            e.invariant
        );
    }
}

/// Every `SimTest` pointer names a comma-separated list of `*.rs` files under this
/// crate's `tests/`; a `CompileFail` pointer is a path relative to `crates/`. A
/// renamed or deleted suite must fail here rather than drift out from under the
/// catalogue unnoticed.
#[test]
fn every_file_pointer_references_a_real_file() {
    let tests_dir = Path::new(env!("CARGO_MANIFEST_DIR")).join("tests");
    let crates_dir = Path::new(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .expect("crate dir has a parent");

    for e in e_catalogue() {
        for v in e.verify {
            match v {
                Verify::SimTest(files) | Verify::Differential(files) => {
                    for file in files.split(',').map(str::trim) {
                        assert!(
                            tests_dir.join(file).exists(),
                            "E{} points at test file {file:?}, which does not exist under {}",
                            e.invariant,
                            tests_dir.display(),
                        );
                    }
                }
                Verify::CompileFail(path) => {
                    assert!(
                        crates_dir.join(path).exists(),
                        "E{} points at compile-fail path {path:?}, which does not exist under {}",
                        e.invariant,
                        crates_dir.display(),
                    );
                }
                Verify::TestFn(_) => panic!(
                    "E{} points at a test function, and this catalogue points at \
                     files: nothing here scans for a function's definition. Give it a \
                     name gate of its own first, as blob-store and machine-entity have.",
                    e.invariant
                ),
                Verify::Checker(_) | Verify::CompileTime(_) | Verify::Deferred(_) => {}
            }
        }
    }
}

#[test]
fn no_entry_claims_a_continuous_checker() {
    // entity's continuous checkers live in `entity::testing`, but each is built
    // with the label its own suite reports under, so there is no fixed global name
    // for this table to cross-check against the way `default_invariants()` gives
    // the core catalogue one. Anyone adding a `Verify::Checker` here must first
    // give entity a named, discoverable checker set — and then this assertion
    // becomes the drift test between the two, as it is in the sandbox catalogue.
    for e in e_catalogue() {
        assert!(
            !e.verify.iter().any(|v| matches!(v, Verify::Checker(_))),
            "E{} claims a continuous checker; entity exposes no named global set",
            e.invariant
        );
    }
}

#[test]
fn invariant_g10_is_verified_by_a_compile_fail_test() {
    let ten = e_catalogue()
        .iter()
        .find(|e| e.invariant == 10)
        .expect("E10 must be catalogued");
    assert!(
        ten.verify
            .iter()
            .any(|v| matches!(v, Verify::CompileFail(_))),
        "E10 (type-safe calls) must be verified by a compile-fail test",
    );
}
