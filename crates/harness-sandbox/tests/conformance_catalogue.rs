//! Catalogue drift test (sandbox spec §6): the machine-readable S1–S5
//! catalogue stays complete and accurate — the same pattern guarding the core,
//! utilities, and harness catalogues.

mod support;

use actor_simulation::Verify;

use support::s_catalogue;

#[test]
fn the_catalogue_covers_s1_through_s5() {
    let numbers: Vec<u8> = s_catalogue().iter().map(|e| e.invariant).collect();
    assert_eq!(numbers, vec![1, 2, 3, 4, 5]);
    for entry in s_catalogue() {
        assert!(
            !entry.verify.is_empty(),
            "S{} has no verification method",
            entry.invariant
        );
    }
}

#[test]
fn no_entry_claims_a_continuous_checker() {
    // S4 is a journal audit at quiescence, never a stream checker: tool
    // execution carries no events for one to consume (harness spec §5.6
    // item 6). Anyone adding a `Verify::Checker` entry here must wire the
    // checker into a live invariant set first — and then this assertion,
    // like its harness sibling, becomes the drift test between the two.
    for entry in s_catalogue() {
        assert!(
            !entry.verify.iter().any(|v| matches!(v, Verify::Checker(_))),
            "S{} claims a continuous checker; none exists in this crate",
            entry.invariant
        );
    }
}
