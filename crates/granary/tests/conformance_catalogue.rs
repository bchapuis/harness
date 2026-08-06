//! Catalogue drift test (granary spec §15): the machine-readable G1–G21 table
//! stays complete, and every file it points at still exists.
//!
//! This is the spec↔code drift gate for granary, the same pattern that guards the
//! core, utilities, harness, and sandbox catalogues. It fails the build if a
//! G-number is missing or duplicated, if an entry claims no verification method,
//! or if a `SimTest`/`CompileFail` pointer names a file that no longer exists —
//! which is what makes §15's "Verified by" column mechanically true rather than
//! merely written down.

mod support;

use std::path::Path;

use actor_simulation::Verify;

use support::catalogue::g_catalogue;

#[test]
fn every_invariant_g1_through_g21_is_present_exactly_once() {
    let mut numbers: Vec<u8> = g_catalogue().iter().map(|e| e.invariant).collect();
    numbers.sort_unstable();
    assert_eq!(
        numbers,
        (1..=21).collect::<Vec<u8>>(),
        "the catalogue must list G1..=G21, each exactly once",
    );
}

#[test]
fn every_entry_has_spec_property_and_a_verification_method() {
    for e in g_catalogue() {
        assert!(
            !e.verify.is_empty(),
            "G{} has no verification method",
            e.invariant
        );
        assert!(
            !e.spec.is_empty() && !e.property.is_empty(),
            "G{} is missing spec or property text",
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

    for e in g_catalogue() {
        for v in e.verify {
            match v {
                Verify::SimTest(files) | Verify::Differential(files) => {
                    for file in files.split(',').map(str::trim) {
                        assert!(
                            tests_dir.join(file).exists(),
                            "G{} points at test file {file:?}, which does not exist under {}",
                            e.invariant,
                            tests_dir.display(),
                        );
                    }
                }
                Verify::CompileFail(path) => {
                    assert!(
                        crates_dir.join(path).exists(),
                        "G{} points at compile-fail path {path:?}, which does not exist under {}",
                        e.invariant,
                        crates_dir.display(),
                    );
                }
                Verify::Checker(_) | Verify::CompileTime(_) => {}
            }
        }
    }
}

#[test]
fn no_entry_claims_a_continuous_checker() {
    // granary's continuous checkers live in `granary::testing`, but each is built
    // with the label its own suite reports under, so there is no fixed global name
    // for this table to cross-check against the way `default_invariants()` gives
    // the core catalogue one. Anyone adding a `Verify::Checker` here must first
    // give granary a named, discoverable checker set — and then this assertion
    // becomes the drift test between the two, as it is in the sandbox catalogue.
    for e in g_catalogue() {
        assert!(
            !e.verify.iter().any(|v| matches!(v, Verify::Checker(_))),
            "G{} claims a continuous checker; granary exposes no named global set",
            e.invariant
        );
    }
}

#[test]
fn invariant_g10_is_verified_by_a_compile_fail_test() {
    let ten = g_catalogue()
        .iter()
        .find(|e| e.invariant == 10)
        .expect("G10 must be catalogued");
    assert!(
        ten.verify
            .iter()
            .any(|v| matches!(v, Verify::CompileFail(_))),
        "G10 (type-safe calls) must be verified by a compile-fail test",
    );
}
