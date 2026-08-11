//! Catalogue drift test (blob-store spec §9): the machine-readable B1–B7 table
//! stays complete, every file it points at still exists, and B4's structural claim
//! still holds.
//!
//! The gate this crate did not have. Its §9 "Verified by" column had drifted to
//! name seven tests that no longer existed — each property still real, each
//! pointer dead — and nothing failed. A renamed or deleted suite now fails here.

mod support;

use std::path::Path;

use actor_simulation::Verify;
use spec::catalogue;

use support::b_catalogue;

#[test]
fn every_invariant_b1_through_b7_is_present_exactly_once() {
    let mut numbers: Vec<u8> = b_catalogue().iter().map(|e| e.invariant).collect();
    numbers.sort_unstable();
    assert_eq!(
        numbers,
        (1..=7).collect::<Vec<u8>>(),
        "the catalogue must list B1..=B7, each exactly once",
    );
}

#[test]
fn every_entry_has_spec_property_and_a_verification_method() {
    for e in b_catalogue() {
        assert!(
            !e.verify.is_empty(),
            "B{} has no verification method",
            e.invariant
        );
        assert!(
            !e.spec.is_empty() && !e.property.is_empty(),
            "B{} is missing spec or property text",
            e.invariant
        );
    }
}

/// A pointer containing a `/` is a path relative to `crates/` — several B-invariants
/// are local properties verified by unit tests beside the code — and a bare filename
/// is relative to this crate's `tests/`.
#[test]
fn every_file_pointer_references_a_real_file() {
    let tests_dir = Path::new(env!("CARGO_MANIFEST_DIR")).join("tests");
    let crates_dir = Path::new(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .expect("crate dir has a parent");

    let sources = crate_sources();

    for e in b_catalogue() {
        for v in e.verify {
            match v {
                Verify::TestFn(names) => {
                    for name in names.split(',').map(str::trim) {
                        assert!(
                            sources.contains(&format!("fn {name}(")),
                            "B{} names test {name:?}, which is not defined in this crate — \
                             a rename left the spec's \"Verified by\" column pointing at nothing",
                            e.invariant,
                        );
                    }
                }
                Verify::SimTest(files) | Verify::Differential(files) => {
                    for file in files.split(',').map(str::trim) {
                        let path = if file.contains('/') {
                            crates_dir.join(file)
                        } else {
                            tests_dir.join(file)
                        };
                        assert!(
                            path.exists(),
                            "B{} points at {file:?}, which does not exist at {}",
                            e.invariant,
                            path.display(),
                        );
                    }
                }
                Verify::CompileFail(path) => {
                    assert!(
                        crates_dir.join(path).exists(),
                        "B{} points at compile-fail path {path:?}, which does not exist",
                        e.invariant,
                    );
                }
                Verify::Checker(_) | Verify::CompileTime(_) | Verify::Deferred(_) => {}
            }
        }
    }
}

/// Every `.rs` file under this crate, concatenated, for the name scan above.
///
/// The catalogue points at test *functions* rather than files, as blob §9 does and
/// for the reason wal §7 gives: several of these cases live in `src/`, beside the
/// code they pin, where asserting that the file still exists asserts nothing.
fn crate_sources() -> String {
    fn walk(dir: &Path, out: &mut String) {
        let Ok(entries) = std::fs::read_dir(dir) else {
            return;
        };
        for entry in entries.filter_map(Result::ok) {
            let path = entry.path();
            if path.is_dir() {
                walk(&path, out);
            } else if path.extension().is_some_and(|x| x == "rs") {
                out.push_str(&std::fs::read_to_string(&path).unwrap_or_default());
                out.push('\n');
            }
        }
    }
    let root = Path::new(env!("CARGO_MANIFEST_DIR"));
    let mut all = String::new();
    walk(&root.join("src"), &mut all);
    walk(&root.join("tests"), &mut all);
    all
}

/// **B4**, checked rather than asserted: the data path runs no consensus because
/// there is no consensus engine in the dependency graph to run.
///
/// `granary` is the tree's consensus-bearing crate (per-shard Raft leadership, the
/// term fence). The blob store deliberately sits beside it on `actor-cluster`
/// alone: content addressing makes concurrent writers of equal content converge
/// without ordering, so nothing needs a term. If a `granary` dependency ever
/// appears here, B4 stops being free and has to be re-argued — this test is where
/// that conversation starts.
#[test]
fn the_data_path_has_no_consensus_engine_to_run() {
    let manifest =
        std::fs::read_to_string(Path::new(env!("CARGO_MANIFEST_DIR")).join("Cargo.toml"))
            .expect("blob-store has a Cargo.toml");

    // Only the real dependency sections; `[dev-dependencies]` may pull in whatever
    // the swarm suites need, and a test-only dependency is not on the data path.
    let runtime: String = manifest
        .split("\n[")
        .filter(|section| section.starts_with("dependencies]") || section.starts_with("target."))
        .collect::<Vec<_>>()
        .join("\n");
    assert!(
        !runtime.is_empty(),
        "expected a [dependencies] section to inspect",
    );
    assert!(
        !runtime.contains("granary"),
        "blob-store gained a `granary` dependency, so B4's \"no consensus on the data path\" \
         is no longer structural — re-verify it or restate the invariant:\n{runtime}",
    );
}

#[test]
fn no_entry_claims_a_continuous_checker() {
    // The B7 swarm checker (`blob-no-resurrection`) is defined inside `swarm.rs`
    // and assembled per-workload, so there is no crate-level named checker set for
    // this table to cross-check against, the way `default_invariants()` gives the
    // core catalogue one. B7 therefore cites the suite that runs it. Anyone adding
    // a `Verify::Checker` here must first expose a discoverable checker set — and
    // then this assertion becomes the drift test between the two.
    for e in b_catalogue() {
        assert!(
            !e.verify.iter().any(|v| matches!(v, Verify::Checker(_))),
            "B{} claims a continuous checker; blob-store exposes no named global set",
            e.invariant
        );
    }
}

/// The table above and the blob-store spec's §9 copy of it are the same
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
    let site = catalogue::site("blob");
    let root = spec::workspace_root(env!("CARGO_MANIFEST_DIR"));
    let documented = catalogue::documented(&root, site).expect("blob-store spec §9 parses");
    let implemented: Vec<catalogue::Row> = b_catalogue()
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
        "blob-store spec §9 and b_catalogue() disagree:\n  {}\n",
        found
            .iter()
            .map(ToString::to_string)
            .collect::<Vec<_>>()
            .join("\n  "),
    );
}
