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
use spec_xref::catalogue;

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

/// Every test the catalogue names is still defined, somewhere across the three
/// crates the machine's verification spans (§7). A rename that misses this table
/// fails here rather than drifting out from under it unnoticed.
#[test]
fn every_named_test_exists() {
    for e in m_catalogue() {
        for name in named_tests(e.verify) {
            assert!(
                defined_in(&name).is_some(),
                "M{} names test {name:?}, which is defined in none of the machine \
                 crates — a rename left the spec's \"Verified by\" column pointing \
                 at nothing",
                e.invariant,
            );
        }
    }
}

/// The test functions an entry names.
fn named_tests(verify: &[Verify]) -> Vec<String> {
    verify
        .iter()
        .filter_map(|v| match v {
            Verify::TestFn(names) => Some(names),
            _ => None,
        })
        .flat_map(|names| names.split(',').map(|n| n.trim().to_string()))
        .collect()
}

/// Where a test function is defined, relative to `crates/`.
///
/// The machine spans `machine-grain`, `machine-frontdoor`, and `machine-host`, and
/// half the point of M4 and M6 is that they are decided outside the grain's own
/// suites — so the search covers the siblings, and the answer says which file.
fn defined_in(name: &str) -> Option<String> {
    fn walk(dir: &Path, needle: &str, out: &mut Option<String>) {
        let Ok(entries) = std::fs::read_dir(dir) else {
            return;
        };
        for entry in entries.filter_map(Result::ok) {
            let path = entry.path();
            if path.is_dir() {
                walk(&path, needle, out);
            } else if out.is_none()
                && path.extension().is_some_and(|x| x == "rs")
                && std::fs::read_to_string(&path).is_ok_and(|s| s.contains(needle))
            {
                *out = Some(path.to_string_lossy().into_owned());
            }
        }
    }
    let crates_dir = Path::new(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .expect("crate dir has a parent");
    let needle = format!("fn {name}(");
    let mut found = None;
    for krate in ["machine-grain", "machine-frontdoor", "machine-host"] {
        walk(&crates_dir.join(krate), &needle, &mut found);
        if found.is_some() {
            break;
        }
    }
    found
}

#[test]
fn the_ingress_and_egress_invariants_are_verified_outside_the_grain_suites() {
    // M4 and M6 are the machine's two seam invariants (§5), and neither is provable
    // from the grain's own simulation: the front door terminates SSH in another
    // crate, and the egress rules are a pure generator with no grain in the loop.
    // Pinning that here keeps a future edit from "simplifying" them into a
    // grain-only pointer that no longer covers what the invariant claims.
    // Asked of where the named tests actually live rather than of how the pointer
    // is spelled: the catalogue names functions, and a function says nothing about
    // its file until you go and find it.
    for invariant in [4u8, 6] {
        let entry = m_catalogue()
            .iter()
            .find(|e| e.invariant == invariant)
            .expect("M4 and M6 must be catalogued");
        let outside: Vec<String> = named_tests(entry.verify)
            .iter()
            .filter_map(|name| defined_in(name))
            .filter(|path| !path.contains("machine-grain/tests/"))
            .collect();
        assert!(
            !outside.is_empty(),
            "M{invariant} is verified only from this crate's tests/, which cannot \
             decide it: {:?}",
            named_tests(entry.verify),
        );
    }
}

/// The table above and the machine spec's §7 copy of it are the same
/// table.
///
/// The rest of this file holds the Rust copy internally consistent. This holds it
/// equal to the prose one, which is the copy a reader trusts: an invariant added
/// to one, or a defining section moved in one, is caught here rather than left to
/// be noticed.
///
/// The suite axis is not compared here: the spec's column names test functions where this table names the files that hold them. So this
/// gate covers the numbering and the defining sections, and the "Verified by"
/// column stays a human's to keep true.
#[test]
fn the_specification_and_this_catalogue_agree() {
    let site = catalogue::site("machine");
    let root = spec_xref::workspace_root(env!("CARGO_MANIFEST_DIR"));
    let documented = catalogue::documented(&root, site).expect("machine spec §7 parses");
    let implemented: Vec<catalogue::Row> = m_catalogue()
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
        "machine spec §7 and m_catalogue() disagree:\n  {}\n",
        found
            .iter()
            .map(ToString::to_string)
            .collect::<Vec<_>>()
            .join("\n  "),
    );
}
