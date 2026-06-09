//! Conformance: the §18.5 invariant catalogue is the single source of truth and
//! stays consistent with the live checker set (spec §17, §18.5, §18.6).
//!
//! This is the spec↔code drift gate. It fails the build if an invariant number
//! is missing or duplicated, if a catalogue `Checker(name)` is not actually
//! present in `default_invariants()`, or if a live checker is not recorded in
//! the catalogue. Keeping it green is what makes the §17 "Verified by" column
//! mechanically true rather than just documented.

use std::collections::BTreeSet;

use actor_simulation::Verify;
use actor_simulation::catalogue;
use actor_simulation::default_invariants;

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

/// Checker names the catalogue claims are continuously checked.
fn catalogue_checker_names() -> BTreeSet<&'static str> {
    catalogue()
        .iter()
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
