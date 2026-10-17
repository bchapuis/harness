//! The architecture page's dependency table is the workspace's dependency graph,
//! and it is written in an order every edge respects.
//!
//! A failure means a `Cargo.toml` and `docs/architecture.md` disagree. Usually
//! the page follows the manifest; when the new edge points down the table, the
//! layering it describes was broken, and that is the conversation to have first.

use std::collections::BTreeSet;
use std::path::PathBuf;

use spec::architecture::NOT_SEAMS;
use spec::architecture::documented;
use spec::architecture::manifests;
use spec::architecture::public_traits;

fn root() -> PathBuf {
    spec::workspace_root(env!("CARGO_MANIFEST_DIR"))
}

#[test]
fn the_page_lists_every_crate_once() {
    let rows = documented(&root()).expect("the architecture page is readable");
    let listed: Vec<&str> = rows.iter().map(|r| r.name.as_str()).collect();
    let unique: BTreeSet<&str> = listed.iter().copied().collect();
    assert_eq!(
        unique.len(),
        listed.len(),
        "a crate is listed twice: {listed:?}"
    );
    let crates: BTreeSet<String> = manifests(&root()).expect("manifests").into_keys().collect();
    let listed: BTreeSet<String> = unique.iter().map(|s| s.to_string()).collect();
    assert_eq!(listed, crates, "the page's crates and crates/ differ");
}

#[test]
fn each_crate_depends_on_what_the_page_says() {
    let manifests = manifests(&root()).expect("manifests");
    let mismatches: Vec<String> = documented(&root())
        .expect("the architecture page is readable")
        .iter()
        .filter_map(|row| {
            let actual = manifests.get(&row.name)?;
            (actual != &row.depends_on).then(|| {
                format!(
                    "{}: the page says {:?}, Cargo.toml says {:?}",
                    row.name, row.depends_on, actual
                )
            })
        })
        .collect();
    assert!(mismatches.is_empty(), "\n{}\n", mismatches.join("\n"));
}

#[test]
fn every_dependency_points_up_the_table() {
    let rows = documented(&root()).expect("the architecture page is readable");
    let mut above: BTreeSet<&str> = BTreeSet::new();
    let mut downward = Vec::new();
    for row in &rows {
        for dep in &row.depends_on {
            let name = dep.trim_end_matches('?');
            if !above.contains(name) {
                downward.push(format!("{} depends on {name}, listed below it", row.name));
            }
        }
        above.insert(row.name.as_str());
    }
    assert!(downward.is_empty(), "\n{}\n", downward.join("\n"));
}

/// A public trait is a seam, and a seam's contract is written down somewhere a
/// reader can be sent: its rustdoc cites the section. The citation gate then
/// holds that section real.
#[test]
fn every_public_trait_cites_its_contract_or_says_why_not() {
    let traits = public_traits(&root()).expect("sources are readable");
    let uncited: Vec<String> = traits
        .iter()
        .filter(|(krate, name, doc)| {
            !doc.contains('§') && !NOT_SEAMS.iter().any(|(k, n, _)| k == krate && n == name)
        })
        .map(|(krate, name, _)| format!("{krate}::{name}"))
        .collect();
    assert!(
        uncited.is_empty(),
        "these public traits cite no spec section and are not in NOT_SEAMS: {uncited:?}",
    );
    for (krate, name, reason) in NOT_SEAMS {
        assert!(!reason.trim().is_empty(), "{krate}::{name} has no reason");
        assert!(
            traits.iter().any(|(k, n, _)| k == krate && n == name),
            "NOT_SEAMS names {krate}::{name}, which is no longer a public trait",
        );
    }
}
