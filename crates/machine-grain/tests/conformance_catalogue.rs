//! Catalogue drift test (machine spec §7): the machine-readable M1–M6 table stays
//! complete, and every file it points at still exists.
//!
//! The machine's spec↔code drift gate, the same pattern guarding the core,
//! utilities, harness, sandbox, and granary catalogues. It fails the build if an
//! M-number is missing or duplicated, if an entry claims no verification method, or
//! if a pointer names a file that no longer exists — which is what keeps §7's
//! "Verified by" column mechanically true rather than merely written down.

mod support;

use std::path::Path;

use actor_simulation::Verify;

use support::m_catalogue;

#[test]
fn every_invariant_m1_through_m6_is_present_exactly_once() {
    let mut numbers: Vec<u8> = m_catalogue().iter().map(|e| e.invariant).collect();
    numbers.sort_unstable();
    assert_eq!(
        numbers,
        (1..=6).collect::<Vec<u8>>(),
        "the catalogue must list M1..=M6, each exactly once",
    );
}

#[test]
fn every_entry_has_spec_property_and_a_verification_method() {
    for e in m_catalogue() {
        assert!(
            !e.verify.is_empty(),
            "M{} has no verification method",
            e.invariant
        );
        assert!(
            !e.spec.is_empty() && !e.property.is_empty(),
            "M{} is missing spec or property text",
            e.invariant
        );
    }
}

/// A pointer containing a `/` is a path relative to `crates/` — the machine's
/// verification spans three crates (§7) — and a bare filename is relative to this
/// crate's `tests/`. Either way a renamed or deleted suite must fail here rather
/// than drift out from under the catalogue unnoticed.
#[test]
fn every_file_pointer_references_a_real_file() {
    let tests_dir = Path::new(env!("CARGO_MANIFEST_DIR")).join("tests");
    let crates_dir = Path::new(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .expect("crate dir has a parent");

    for e in m_catalogue() {
        for v in e.verify {
            match v {
                Verify::SimTest(files) | Verify::Differential(files) => {
                    for file in files.split(',').map(str::trim) {
                        let path = if file.contains('/') {
                            crates_dir.join(file)
                        } else {
                            tests_dir.join(file)
                        };
                        assert!(
                            path.exists(),
                            "M{} points at {file:?}, which does not exist at {}",
                            e.invariant,
                            path.display(),
                        );
                    }
                }
                Verify::CompileFail(path) => {
                    assert!(
                        crates_dir.join(path).exists(),
                        "M{} points at compile-fail path {path:?}, which does not exist",
                        e.invariant,
                    );
                }
                Verify::Checker(_) | Verify::CompileTime(_) => {}
            }
        }
    }
}

#[test]
fn the_ingress_and_egress_invariants_are_verified_outside_the_grain_suites() {
    // M4 and M6 are the machine's two seam invariants (§5), and neither is provable
    // from the grain's own simulation: the front door terminates SSH in another
    // crate, and the egress rules are a pure generator with no grain in the loop.
    // Pinning that here keeps a future edit from "simplifying" them into a
    // grain-only pointer that no longer covers what the invariant claims.
    for invariant in [4u8, 6] {
        let entry = m_catalogue()
            .iter()
            .find(|e| e.invariant == invariant)
            .expect("M4 and M6 must be catalogued");
        assert!(
            entry.verify.iter().any(|v| match v {
                Verify::SimTest(files) | Verify::Differential(files) =>
                    files.split(',').any(|f| f.trim().contains('/')),
                _ => false,
            }),
            "M{invariant} must cite verification outside this crate's tests/",
        );
    }
}
