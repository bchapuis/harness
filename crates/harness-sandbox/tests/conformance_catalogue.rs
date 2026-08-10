//! Catalogue drift test (sandbox spec §6): the machine-readable S1–S5
//! catalogue stays complete and accurate — the same pattern guarding the core,
//! utilities, and harness catalogues.

mod support;

use std::path::Path;

use actor_simulation::Verify;
use spec_xref::catalogue;

use support::s_catalogue;

/// Every suite the catalogue names still exists.
///
/// The sibling catalogues have had this gate all along; this one could not, because
/// its pointers were prose with the file names buried in them and nothing could tell
/// a renamed suite from a reworded sentence. Now that they are file lists, a rename
/// that misses this table fails the build here, as it does everywhere else.
///
/// A bare name is a file under this crate's `tests/`; a name with a slash is
/// relative to `crates/`, which is how S4 reaches the harness suite that owns it.
#[test]
fn every_file_pointer_references_a_real_file() {
    let tests_dir = Path::new(env!("CARGO_MANIFEST_DIR")).join("tests");
    let crates_dir = Path::new(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .expect("crate dir has a parent");

    for entry in s_catalogue() {
        for v in entry.verify {
            let files = match v {
                Verify::SimTest(files) | Verify::Differential(files) => files,
                Verify::CompileFail(_)
                | Verify::Checker(_)
                | Verify::CompileTime(_)
                | Verify::Deferred(_) => continue,
            };
            for file in files.split(',').map(str::trim) {
                let path = if file.contains('/') {
                    crates_dir.join(file)
                } else {
                    tests_dir.join(file)
                };
                assert!(
                    path.exists(),
                    "S{} points at {file:?}, which does not exist at {}",
                    entry.invariant,
                    path.display(),
                );
            }
        }
    }
}

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
