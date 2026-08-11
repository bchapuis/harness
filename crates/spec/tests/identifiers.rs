//! The spec↔code identifier gate: every Rust name the specifications use is still
//! declared somewhere under `crates/`.
//!
//! The third of the three gates in this crate, and the loosest. The citation gate
//! resolves `§` references exactly; the catalogue gate compares two tables field
//! by field. This one only asks whether a name still exists, because the index
//! behind it is a scan rather than a compiler (see the module docs for why).
//!
//! A failure means one of three things, in rough order of likelihood: the code was
//! renamed and the prose was not; the spec always meant something the tree does
//! not define, and belongs in `EXEMPT` with its reason; or the scan does not
//! recognize how the name is declared, and `harvest` needs to.

use spec::identifiers::EXEMPT;
use spec::identifiers::Index;
use spec::identifiers::Why;
use spec::identifiers::stale;
use spec::identifiers::unknown;
use spec::workspace_root;

fn index() -> Index {
    Index::build(&workspace_root(env!("CARGO_MANIFEST_DIR"))).expect("crates/ is readable")
}

#[test]
fn every_identifier_the_specs_name_is_declared() {
    let root = workspace_root(env!("CARGO_MANIFEST_DIR"));
    let index = index();
    let found = unknown(&root, &index).expect("every scanned document is readable");
    assert!(
        found.is_empty(),
        "{} identifier{} in the specs name nothing in the tree:\n\n{}\n",
        found.len(),
        if found.len() == 1 { "" } else { "s" },
        found
            .iter()
            .map(ToString::to_string)
            .collect::<Vec<_>>()
            .join("\n\n"),
    );
}

/// The exemption list is an assertion of absence, so it is checked that way. An
/// entry the tree has started to define has outlived its reason and must go —
/// which is what keeps the list from becoming a place to hide failures.
#[test]
fn no_exemption_has_outlived_its_reason() {
    let found = stale(&index());
    assert!(
        found.is_empty(),
        "these exemptions name things the tree now declares; delete them:\n{}",
        found
            .iter()
            .map(|x| format!("  {} — was exempt because: {}", x.name, x.reason))
            .collect::<Vec<_>>()
            .join("\n"),
    );
}

/// An index that stopped finding declarations would make the gate above pass by
/// knowing everything. The floor is far under the real count and exists to catch
/// a scan that has broken, not to pin a figure.
#[test]
fn the_index_still_finds_the_tree() {
    let index = index();
    assert!(
        index.len() > 2_000,
        "only {} names indexed under crates/; the scan has regressed",
        index.len(),
    );
    for name in ["GrainHandler", "GrainRef", "Verify", "CatalogueEntry"] {
        assert!(index.contains(name), "{name} should be indexed");
    }
}

/// Every kind of exemption is represented, and each says why. A `DeclaredAbsent`
/// entry is the interesting one: it records that a spec names something in order
/// to say the tree does not have it.
#[test]
fn the_exemptions_are_accounted_for() {
    assert!(
        EXEMPT.iter().any(|x| x.why == Why::DeclaredAbsent),
        "the list should still carry the deliberate absences",
    );
    for x in EXEMPT {
        assert!(!x.reason.trim().is_empty(), "{} has no reason", x.name);
    }
}

/// Prints what the gate sees. Run with `--ignored --nocapture` when the scan
/// changes.
#[test]
#[ignore = "diagnostic"]
fn dump() {
    let root = workspace_root(env!("CARGO_MANIFEST_DIR"));
    let index = index();
    println!("indexed {} names", index.len());
    for u in unknown(&root, &index).expect("readable") {
        println!("{u}");
    }
}
