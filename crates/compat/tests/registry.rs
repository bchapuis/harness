//! The format registry in `docs/compatibility-spec.md` and the formats the code
//! declares are the same set, at the same versions.
//!
//! Every format is a `Format::new("<name>", <version>)` somewhere in the
//! workspace. This scans each crate's `src/` tree for that call, outside comments
//! and test modules (so a fixture such as `wal.test` is not a claim), and reads the
//! registry table's first two columns; each side must hold exactly the other. A
//! source scan rather than an import, because this crate depends on nothing in the
//! tree and every crate that declares a format depends on it.

use std::collections::BTreeMap;
use std::fs;
use std::path::Path;
use std::path::PathBuf;

/// The workspace root, two levels above this crate.
fn root() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .ancestors()
        .nth(2)
        .expect("the crate lives at crates/compat")
        .to_path_buf()
}

/// Every `.rs` file under `dir`, recursively.
fn sources(dir: &Path, out: &mut Vec<PathBuf>) {
    let Ok(entries) = fs::read_dir(dir) else {
        return;
    };
    for entry in entries.flatten() {
        let path = entry.path();
        if path.is_dir() {
            sources(&path, out);
        } else if path.extension().is_some_and(|e| e == "rs") {
            out.push(path);
        }
    }
}

/// The formats a source file declares, by name and version.
fn declared_in(text: &str, out: &mut BTreeMap<String, u16>, file: &Path) {
    const CALL: &str = "Format::new(\"";
    let mut gated = false;
    for line in text.lines() {
        let line = line.trim_start();
        // The unit tests sit in a trailing `#[cfg(test)] mod`; nothing after it
        // declares a format the tree ships.
        if gated && line.starts_with("mod ") {
            break;
        }
        gated = line.starts_with("#[cfg(test)]");
        if line.starts_with("//") {
            continue;
        }
        let Some(at) = line.find(CALL) else {
            continue;
        };
        let rest = &line[at + CALL.len()..];
        let (name, rest) = rest
            .split_once('"')
            .unwrap_or_else(|| panic!("{}: unterminated format name", file.display()));
        let version: String = rest
            .trim_start_matches([',', ' '])
            .chars()
            .take_while(char::is_ascii_digit)
            .collect();
        let version = version
            .parse()
            .unwrap_or_else(|_| panic!("{}: {name} has no literal version", file.display()));
        if let Some(other) = out.insert(name.to_string(), version) {
            panic!("{name} is declared twice (v{other} and v{version}); a format is declared once");
        }
    }
}

/// The registry table's rows, by name and version.
fn registered(spec: &str) -> BTreeMap<String, u16> {
    let table = spec
        .split_once("## 4. The registry")
        .expect("the compatibility spec has a registry section")
        .1;
    table
        .lines()
        .filter_map(|line| line.strip_prefix("| `"))
        .map(|row| {
            let (name, rest) = row.split_once('`').expect("a backticked format name");
            let version = rest
                .trim_start_matches([' ', '|'])
                .split(' ')
                .next()
                .and_then(|v| v.parse().ok())
                .unwrap_or_else(|| panic!("registry row {name} has no version"));
            (name.to_string(), version)
        })
        .collect()
}

#[test]
fn the_registry_lists_exactly_the_formats_the_code_declares() {
    let root = root();
    let mut files = Vec::new();
    for krate in fs::read_dir(root.join("crates"))
        .expect("crates/ is readable")
        .flatten()
    {
        sources(&krate.path().join("src"), &mut files);
    }
    let mut declared = BTreeMap::new();
    for file in &files {
        let text = fs::read_to_string(file).expect("a source file is readable");
        declared_in(&text, &mut declared, file);
    }

    let spec = fs::read_to_string(root.join("docs/compatibility-spec.md"))
        .expect("docs/compatibility-spec.md is readable");
    let registered = registered(&spec);

    assert!(!declared.is_empty(), "the scan found no formats at all");
    assert_eq!(
        registered, declared,
        "the compatibility spec's registry (left) and the formats the code declares \
         (right) disagree: a new format must be registered, and a retired one removed",
    );
}
