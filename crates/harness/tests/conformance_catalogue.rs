//! Catalogue drift test (harness spec §11): the machine-readable H1–H8
//! catalogue and the live checker set must agree — a checker added in code
//! but not recorded in the catalogue (or vice versa) fails the build, the
//! same pattern guarding the core and utilities catalogues.

mod support;

use std::collections::BTreeSet;

use actor_simulation::Verify;
use actor_simulation::default_invariants;

use support::harness_catalogue;
use support::harness_invariants;

#[test]
fn the_catalogue_and_the_checker_set_agree() {
    // Names of the harness-specific continuous checkers: the harness set
    // minus the core defaults.
    let core: BTreeSet<String> = default_invariants()
        .iter()
        .map(|i| i.name().to_string())
        .collect();
    let live: BTreeSet<String> = harness_invariants()
        .iter()
        .map(|i| i.name().to_string())
        .filter(|name| !core.contains(name))
        .collect();

    let catalogued: BTreeSet<String> = harness_catalogue()
        .iter()
        .flat_map(|entry| entry.verify.iter())
        .filter_map(|verify| match verify {
            Verify::Checker(name) => Some(name.to_string()),
            _ => None,
        })
        .collect();

    assert_eq!(
        live, catalogued,
        "continuous harness checkers and the H catalogue drifted apart (§11)"
    );
}

#[test]
fn the_catalogue_covers_h1_through_h8() {
    let numbers: Vec<u8> = harness_catalogue().iter().map(|e| e.invariant).collect();
    assert_eq!(numbers, vec![1, 2, 3, 4, 5, 6, 7, 8]);
    for entry in harness_catalogue() {
        assert!(
            !entry.verify.is_empty(),
            "H{} has no verification method",
            entry.invariant
        );
    }
}
