//! Guard rails on the names that sweeps and the corpus agree about (spec §18.6).
//!
//! A corpus entry is keyed by a workload's `name()`. Two properties have to hold
//! for that key to mean anything, and neither is enforced by the type system
//! because the names live in test binaries the library never sees:
//!
//! 1. **Names are unique tree-wide.** Two workloads sharing a name would make
//!    one replay the other's regressions — silently, and in the direction of
//!    less testing on whichever one did not need them.
//! 2. **Every corpus key names a real workload.** A typo in `corpus.txt` is
//!    otherwise invisible: the key matches nothing, its seed never runs, and the
//!    ratchet quietly stops holding for that regression.
//!
//! This test scans the workspace's test sources for the two ways a sweep gets a
//! name — a `Workload`/`ClusterWorkload` impl, and `scenario_sweep`'s literal —
//! and checks both properties. It is the same shape as the catalogue drift gate:
//! a cheap source-level assertion standing in for a check the compiler cannot
//! make across binary boundaries.

use std::collections::BTreeMap;
use std::fs;
use std::path::Path;
use std::path::PathBuf;

/// Every `*.rs` under any `crates/*/tests/` directory.
fn test_sources() -> Vec<PathBuf> {
    let root = Path::new(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .expect("crates/")
        .to_path_buf();
    let mut out = Vec::new();
    let crates = fs::read_dir(&root).expect("read crates/");
    for entry in crates.flatten() {
        let tests = entry.path().join("tests");
        if !tests.is_dir() {
            continue;
        }
        collect_rs(&tests, &mut out);
    }
    // Not this file: it carries the marker strings as literals, and would
    // otherwise report itself as defining every name it knows how to find.
    let this = Path::new(file!())
        .file_name()
        .expect("this file has a name")
        .to_owned();
    out.retain(|p| p.file_name() != Some(this.as_os_str()));
    out.sort();
    assert!(!out.is_empty(), "found no test sources under {root:?}");
    out
}

fn collect_rs(dir: &Path, out: &mut Vec<PathBuf>) {
    for entry in fs::read_dir(dir).expect("read tests dir").flatten() {
        let path = entry.path();
        if path.is_dir() {
            collect_rs(&path, out);
        } else if path.extension().is_some_and(|e| e == "rs") {
            out.push(path);
        }
    }
}

/// The first string literal after `from` in `src`, and where it ended.
fn literal_after(src: &str, from: usize) -> Option<(String, usize)> {
    let open = from + src[from..].find('"')?;
    let rest = &src[open + 1..];
    let close = rest.find('"')?;
    Some((rest[..close].to_string(), open + 1 + close))
}

/// Every string literal inside the block that starts at the first `{` after
/// `from`. A `name()` is often a `match` over modes rather than one literal —
/// `singleton-chaos/leader` is a fourth arm — so taking only the first would
/// leave most of a workload's names unknown to the corpus check.
fn literals_in_block(src: &str, from: usize) -> Vec<String> {
    let Some(open) = src[from..].find('{').map(|i| from + i) else {
        return Vec::new();
    };
    let bytes = src.as_bytes();
    let (mut depth, mut i, mut out) = (0usize, open, Vec::new());
    while i < bytes.len() {
        match bytes[i] {
            b'{' => depth += 1,
            b'}' => {
                depth -= 1;
                if depth == 0 {
                    break;
                }
            }
            b'"' => {
                if let Some((literal, end)) = literal_after(src, i) {
                    out.push(literal);
                    i = end;
                }
            }
            _ => {}
        }
        i += 1;
    }
    out
}

/// Sweep names defined in a source file, each with the line it appears on.
///
/// Two shapes carry a name: a `name()` method inside a `Workload` or
/// `ClusterWorkload` impl, and the first argument of `scenario_sweep`. Invariant
/// impls also have `name()`, but those are violation labels rather than corpus
/// keys, so only workload impls count.
fn names_in(src: &str) -> Vec<(String, usize)> {
    let line_of = |byte: usize| src[..byte].matches('\n').count() + 1;
    let mut found = Vec::new();

    for marker in ["impl Workload for", "impl ClusterWorkload for"] {
        let mut at = 0;
        while let Some(hit) = src[at..].find(marker) {
            let start = at + hit;
            // `name()` is the first method in these impls by convention; bound
            // the search so a later impl's name is never attributed here.
            let window_end = (start + 800).min(src.len());
            if let Some(name_at) = src[start..window_end].find("fn name(") {
                for name in literals_in_block(src, start + name_at) {
                    found.push((name, line_of(start)));
                }
            }
            at = start + marker.len();
        }
    }

    let mut at = 0;
    while let Some(hit) = src[at..].find("scenario_sweep(") {
        let start = at + hit;
        if let Some((name, end)) = literal_after(src, start) {
            found.push((name, line_of(start)));
            at = end;
        } else {
            at = start + "scenario_sweep(".len();
        }
    }
    found
}

/// Every sweep name in the workspace, mapped to where it is defined.
fn all_names() -> BTreeMap<String, Vec<String>> {
    let mut index: BTreeMap<String, Vec<String>> = BTreeMap::new();
    for path in test_sources() {
        let src = fs::read_to_string(&path).expect("read test source");
        let file = path
            .components()
            .rev()
            .take(3)
            .collect::<Vec<_>>()
            .iter()
            .rev()
            .map(|c| c.as_os_str().to_string_lossy())
            .collect::<Vec<_>>()
            .join("/");
        for (name, line) in names_in(&src) {
            index
                .entry(name)
                .or_default()
                .push(format!("{file}:{line}"));
        }
    }
    index
}

#[test]
fn sweep_names_are_unique_across_the_workspace() {
    let index = all_names();
    let collisions: Vec<_> = index.iter().filter(|(_, at)| at.len() > 1).collect();
    assert!(
        collisions.is_empty(),
        "sweep names must be unique — a shared name makes one workload replay \
         another's corpus seeds:\n{}",
        collisions
            .iter()
            .map(|(name, at)| format!("  '{name}' at {}", at.join(", ")))
            .collect::<Vec<_>>()
            .join("\n"),
    );
    // A floor, so a refactor that silently stops matching any name fails here
    // rather than passing vacuously.
    assert!(
        index.len() >= 30,
        "only found {} sweep names; the scanner has probably stopped matching",
        index.len(),
    );
}

/// The sweep runners, split by which filenames may host them
/// (docs/simulation-testing.md, "Where things live"). `scenario_sweep` is
/// deliberately absent: it is unconstrained by clause 3.
const INVARIANT_RUNNERS: &[&str] = &[
    "run_swarm(",
    "run_cluster_swarm(",
    "run_cluster_swarm_coverage(",
];
const DETERMINISM_RUNNERS: &[&str] = &[
    "replay_swarm(",
    "replay_cluster_swarm(",
    "check_reproducible(",
    "check_cluster_reproducible(",
];

/// Whether `src` calls any of `runners` — as a call, not as an import, so a
/// `use actor_simulation::run_swarm;` line does not count as hosting a sweep.
fn calls_any(src: &str, runners: &[&str]) -> bool {
    src.lines()
        .filter(|line| !line.trim_start().starts_with("use "))
        .any(|line| runners.iter().any(|r| line.contains(r)))
}

#[test]
fn sweeps_live_in_swarm_files() {
    // Clause 1: an invariant/coverage sweep lives in `*swarm.rs`.
    // Clause 2: a reproducibility sweep lives in `*swarm.rs` or `*determinism.rs`.
    // Clause 3: `scenario_sweep` is unconstrained and is not checked here.
    //
    // A source-level assertion, like the two name gates below: the compiler
    // cannot see across test-binary boundaries, and a convention nothing checks
    // is one the tree drifts away from — which is exactly what happened before
    // this test existed.
    let mut misplaced = Vec::new();
    let (mut invariant_files, mut determinism_files) = (0usize, 0usize);

    for path in test_sources() {
        let src = fs::read_to_string(&path).expect("read test source");
        let name = path
            .file_name()
            .expect("test source has a name")
            .to_string_lossy()
            .to_string();
        let is_swarm = name.ends_with("swarm.rs");
        let is_determinism = name.ends_with("determinism.rs");

        if calls_any(&src, INVARIANT_RUNNERS) {
            invariant_files += 1;
            if !is_swarm {
                misplaced.push(format!(
                    "  {name}: runs an invariant/coverage sweep but is not named `*swarm.rs`"
                ));
            }
        }
        if calls_any(&src, DETERMINISM_RUNNERS) {
            determinism_files += 1;
            if !is_swarm && !is_determinism {
                misplaced.push(format!(
                    "  {name}: runs a reproducibility sweep but is not named \
                     `*swarm.rs` or `*determinism.rs`"
                ));
            }
        }
    }

    assert!(
        misplaced.is_empty(),
        "sweeps must live in sweep files — a sweep failure names a seed to \
         replay, not a sequence to read, and mixing the two makes a slow sweep \
         sit between you and a fast scenario suite \
         (docs/simulation-testing.md):\n{}",
        misplaced.join("\n"),
    );
    // Floors, so a scanner that silently stops matching fails here rather than
    // passing vacuously — the same guard as the name gate below.
    assert!(
        invariant_files >= 8 && determinism_files >= 4,
        "only found {invariant_files} invariant-sweep and {determinism_files} \
         reproducibility-sweep files; the scanner has probably stopped matching",
    );
}

#[test]
fn every_corpus_key_names_a_real_workload() {
    let known = all_names();
    let corpus = fs::read_to_string(Path::new(env!("CARGO_MANIFEST_DIR")).join("corpus.txt"))
        .expect("read corpus.txt");

    let mut unknown = Vec::new();
    for (number, raw) in corpus.lines().enumerate() {
        let line = raw.split('#').next().unwrap_or("").trim();
        if line.is_empty() {
            continue;
        }
        let key = line.split_whitespace().next().expect("non-empty line");
        if !known.contains_key(key) {
            unknown.push(format!("  line {}: '{key}'", number + 1));
        }
    }
    assert!(
        unknown.is_empty(),
        "corpus.txt names workloads that do not exist — these seeds never run, \
         so the regression they record is not actually guarded:\n{}",
        unknown.join("\n"),
    );
}
