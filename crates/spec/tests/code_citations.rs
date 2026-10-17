//! The code half of the citation gate: every `§` citation in the workspace's Rust
//! sources resolves to a section that exists.
//!
//! The crates cite the specifications several thousand times, more than the specs
//! cite each other, and a renumbered section breaks a comment exactly as silently
//! as it breaks a paragraph. A failure here is fixed where `citations.rs` says:
//! usually the citation follows the section that moved; occasionally the crate
//! started citing a spec bare that its [`spec::CRATES`] entry does not name.

use std::path::Path;
use std::path::PathBuf;

use spec::CRATES;
use spec::UNSCANNED_CRATES;
use spec::citations::Registry;

fn root() -> PathBuf {
    spec::workspace_root(env!("CARGO_MANIFEST_DIR"))
}

#[test]
fn every_citation_in_code_resolves() {
    let root = root();
    let registry = Registry::load(&root).expect("every document in DOCUMENTS is readable");
    let unresolved = registry
        .unresolved_in_code(&root)
        .expect("every crate's sources are readable");
    assert!(
        unresolved.is_empty(),
        "{} citations in code resolve to no section:\n\n{}\n",
        unresolved.len(),
        unresolved
            .iter()
            .map(ToString::to_string)
            .collect::<Vec<_>>()
            .join("\n\n"),
    );
}

/// A crate absent from `CRATES` is read by nothing, so its citations rot unseen.
#[test]
fn every_crate_is_scanned_or_says_why_not() {
    let root = root();
    let mut missing: Vec<String> = std::fs::read_dir(root.join("crates"))
        .expect("crates/ exists")
        .filter_map(Result::ok)
        .filter(|e| Path::new(&e.path()).join("Cargo.toml").exists())
        .filter_map(|e| e.file_name().into_string().ok())
        .filter(|name| {
            !CRATES.iter().any(|c| c.name == name)
                && !UNSCANNED_CRATES.iter().any(|(n, _)| n == name)
        })
        .collect();
    missing.sort();
    assert!(
        missing.is_empty(),
        "these crates are neither in CRATES nor in UNSCANNED_CRATES: {missing:?}",
    );
    for krate in CRATES {
        assert!(
            root.join("crates")
                .join(krate.name)
                .join("Cargo.toml")
                .exists(),
            "CRATES names {}, which is not a crate",
            krate.name,
        );
        for id in krate.cites {
            assert!(
                spec::DOCUMENTS.iter().any(|d| d.id == *id),
                "{}'s bare citations resolve against {id:?}, which is not registered",
                krate.name,
            );
        }
    }
}

/// The code half of `the_gate_still_has_something_to_check`: a masking or walking
/// regression that reads no comments would pass the gate above vacuously.
#[test]
fn the_code_gate_still_has_something_to_check() {
    let found: usize = spec::citations::rust_sources(&root())
        .expect("every crate's sources are readable")
        .iter()
        .map(|(_, _, text)| spec::citations::citations(&spec::citations::rust_comments(text)).len())
        .sum();
    assert!(
        found > 1_000,
        "only {found} citations found across crates/; the scanner has regressed"
    );
}
