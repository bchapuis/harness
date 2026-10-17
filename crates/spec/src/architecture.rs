//! The architecture page's dependency table, read against the manifests.
//!
//! `docs/architecture.md` lists every crate with the workspace crates it depends
//! on, in an order where each depends only on crates above it. That table is the
//! one-page answer to "what may this crate use", and it rots exactly like the
//! prose around it: one `Cargo.toml` edit adds an edge the page does not show.
//! This module reads both sides so `tests/architecture.rs` can hold them equal,
//! and hold the order honest.

use std::collections::BTreeMap;
use std::collections::BTreeSet;
use std::io;
use std::path::Path;

use crate::markdown;

/// The architecture page, relative to the repository root.
pub const PAGE: &str = "docs/architecture.md";

/// One row of the page's dependency table: a crate and what it depends on, each
/// dependency written as its crate name with a trailing `?` when optional.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Row {
    /// The crate's directory name under `crates/`.
    pub name: String,
    /// Its normal dependencies on workspace crates.
    pub depends_on: BTreeSet<String>,
}

/// The rows of the first table on the page whose header is `Crate` and
/// `Depends on`, in page order.
pub fn documented(root: &Path) -> io::Result<Vec<Row>> {
    let text = std::fs::read_to_string(root.join(PAGE))?;
    let mut lines = text
        .lines()
        .skip_while(|l| !(l.starts_with("| Crate |") && l.contains("Depends on")))
        .skip(1)
        .skip_while(|l| l.starts_with("|---"));
    let mut rows = Vec::new();
    for line in lines.by_ref().take_while(|l| l.starts_with('|')) {
        let cells: Vec<&str> = line.trim().trim_matches('|').split('|').collect();
        let Some(name) = cells.first().and_then(|c| markdown::backticked(c).next()) else {
            continue;
        };
        let depends_on = cells
            .get(1)
            .map(|c| markdown::backticked(c).map(str::to_string).collect())
            .unwrap_or_default();
        rows.push(Row {
            name: name.to_string(),
            depends_on,
        });
    }
    Ok(rows)
}

/// Every crate under `crates/` with its normal dependencies on other workspace
/// crates, read from its manifest's `[dependencies]` tables (target-specific ones
/// included), optional ones marked with a trailing `?`.
pub fn manifests(root: &Path) -> io::Result<BTreeMap<String, BTreeSet<String>>> {
    let mut texts = BTreeMap::new();
    for entry in std::fs::read_dir(root.join("crates"))? {
        let path = entry?.path().join("Cargo.toml");
        if let Ok(text) = std::fs::read_to_string(&path)
            && let Some(name) = path
                .parent()
                .and_then(Path::file_name)
                .and_then(|n| n.to_str())
        {
            texts.insert(name.to_string(), text);
        }
    }
    let workspace: BTreeSet<String> = texts.keys().cloned().collect();
    Ok(texts
        .iter()
        .map(|(name, text)| (name.clone(), normal_dependencies(text, &workspace)))
        .collect())
}

/// The workspace crates a manifest's normal dependency tables name.
fn normal_dependencies(manifest: &str, workspace: &BTreeSet<String>) -> BTreeSet<String> {
    let mut out = BTreeSet::new();
    let mut in_dependencies = false;
    for line in manifest.lines().map(str::trim) {
        if line.starts_with('[') {
            in_dependencies = line == "[dependencies]"
                || (line.starts_with("[target.") && line.ends_with(".dependencies]"));
            continue;
        }
        if !in_dependencies || line.starts_with('#') {
            continue;
        }
        let key = line.split(['=', '.', ' ']).next().unwrap_or("").trim();
        if workspace.contains(key) {
            let optional = line.contains("optional = true");
            out.insert(if optional {
                format!("{key}?")
            } else {
                key.to_string()
            });
        }
    }
    out
}

/// The public traits that are not seams, each with why. Every other `pub trait`
/// in the workspace is a seam, and its rustdoc cites the section that states its
/// contract (`tests/architecture.rs`).
pub const NOT_SEAMS: &[(&str, &str, &str)] = &[
    (
        "actor-simulation",
        "Model",
        "the sequential model a linearizability check replays against, a test oracle",
    ),
    (
        "entity",
        "EntityExt",
        "adds the `EntityRef` constructors to every `EntitySystem`; the seam is `EntitySystem`",
    ),
    (
        "entity",
        "DynEntityJournal",
        "the object-safe twin of `EntityJournal`, which carries the contract",
    ),
    (
        "entity",
        "Sealed",
        "seals the facet seam so only this crate implements `Facet`",
    ),
    (
        "machine-host",
        "Duplex",
        "names the byte stream a guest channel is, an alias over tokio's traits",
    ),
];

/// Every `pub trait` declared under a crate's `src/`, with the doc comment above
/// it: `(crate, trait, doc)`.
pub fn public_traits(root: &Path) -> io::Result<Vec<(String, String, String)>> {
    let mut out = Vec::new();
    for (krate, path, text) in crate::citations::rust_sources(root)? {
        if !path.starts_with(root.join("crates").join(krate.name).join("src")) {
            continue;
        }
        let lines: Vec<&str> = text.lines().collect();
        for (i, line) in lines.iter().enumerate() {
            let Some(rest) = line.trim_start().strip_prefix("pub trait ") else {
                continue;
            };
            let name: String = rest
                .chars()
                .take_while(|c| c.is_alphanumeric() || *c == '_')
                .collect();
            let doc: Vec<&str> = lines[..i]
                .iter()
                .rev()
                .take_while(|l| {
                    let t = l.trim_start();
                    t.starts_with("///") || t.starts_with("#[")
                })
                .copied()
                .collect();
            out.push((krate.name.to_string(), name, doc.join("\n")));
        }
    }
    Ok(out)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_manifest_yields_its_workspace_dependencies_only() {
        let workspace: BTreeSet<String> = ["entity", "wal", "agent"].map(String::from).into();
        let manifest = "[package]\nname = \"x\"\n\n[dependencies]\nentity.workspace = true\n\
                        serde.workspace = true\nwal = { workspace = true, optional = true }\n\n\
                        [dev-dependencies]\nagent.workspace = true\n";
        assert_eq!(
            normal_dependencies(manifest, &workspace),
            ["entity".to_string(), "wal?".to_string()].into(),
        );
    }
}
