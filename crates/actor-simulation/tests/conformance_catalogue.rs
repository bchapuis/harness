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

use actor_simulation::Verify;
use actor_simulation::catalogue;
use actor_simulation::default_invariants;
use actor_simulation::utilities_catalogue;

#[test]
fn every_invariant_1_through_22_is_present_exactly_once() {
    let mut numbers: Vec<u8> = catalogue().iter().map(|e| e.invariant).collect();
    numbers.sort_unstable();
    assert_eq!(
        numbers,
        (1..=22).collect::<Vec<u8>>(),
        "catalogue must list invariants #1..=#22, each exactly once"
    );
}

#[test]
fn every_entry_has_spec_property_and_a_verification_method() {
    for e in catalogue() {
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

/// Checker names the catalogues (core and utilities) claim are continuously
/// checked.
fn catalogue_checker_names() -> BTreeSet<&'static str> {
    catalogue()
        .iter()
        .chain(utilities_catalogue())
        .flat_map(|e| e.verify.iter())
        .filter_map(|v| match v {
            Verify::Checker(name) => Some(*name),
            _ => None,
        })
        .collect()
}

/// Checker names actually wired into `default_invariants()`.
fn live_checker_names() -> BTreeSet<&'static str> {
    default_invariants().iter().map(|i| i.name()).collect()
}

#[test]
fn every_catalogue_checker_is_live() {
    let live = live_checker_names();
    for name in catalogue_checker_names() {
        assert!(
            live.contains(name),
            "catalogue records checker {name:?}, but it is not in default_invariants()"
        );
    }
}

#[test]
fn every_live_checker_is_recorded_in_the_catalogue() {
    let recorded = catalogue_checker_names();
    for name in live_checker_names() {
        assert!(
            recorded.contains(name),
            "default_invariants() ships checker {name:?} that the catalogue does not record"
        );
    }
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

    for e in catalogue().iter().chain(utilities_catalogue()) {
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
                Verify::Checker(_) | Verify::CompileTime(_) => {}
            }
        }
    }
}

#[test]
fn invariant_20_is_verified_by_a_compile_fail_test() {
    let twenty = catalogue()
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
