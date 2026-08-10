//! Catalogue drift test (sandbox spec §6): the machine-readable S1–S5
//! catalogue stays complete and accurate — the same pattern guarding the core,
//! utilities, and harness catalogues.

mod support;

use actor_simulation::Verify;
use spec_xref::catalogue;

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

/// The table above and the sandbox spec's §6 copy of it are the same
/// table.
///
/// The rest of this file holds the Rust copy internally consistent. This holds it
/// equal to the prose one, which is the copy a reader trusts: an invariant added
/// to one, or a defining section moved in one, is caught here rather than left to
/// be noticed.
#[test]
fn the_specification_and_this_catalogue_agree() {
    let site = catalogue::site("sandbox");
    let root = spec_xref::workspace_root(env!("CARGO_MANIFEST_DIR"));
    let documented = catalogue::documented(&root, site).expect("sandbox spec §6 parses");
    let implemented: Vec<catalogue::Row> = s_catalogue()
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
        "sandbox spec §6 and s_catalogue() disagree:\n  {}\n",
        found
            .iter()
            .map(ToString::to_string)
            .collect::<Vec<_>>()
            .join("\n  "),
    );
}
