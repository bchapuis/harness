//! The fixed-seed regression corpus (spec §18.6).
//!
//! A swarm sweep is a *sample* of the seed space, so a bug it once found is not
//! one it keeps finding: widen the base, narrow the width, and the seed that
//! caught it is no longer in the sample. Every seed recorded in `corpus.txt`
//! therefore replays on every run of its workload (local, CI, and soak alike) on
//! top of whatever that run's sweep covers. See `docs/simulation-testing.md`.
//!
//! Corpus seeds are *absolute*. `SWARM_SEED_BASE` moves a sweep to unexplored
//! ground; it MUST NOT move the regressions.

use std::collections::BTreeMap;
use std::sync::OnceLock;

/// Compiled in rather than read at runtime: a test binary must not depend on
/// its working directory to know what it is required to check, and a corpus
/// that failed to load would silently weaken every sweep.
const CORPUS: &str = include_str!("../corpus.txt");

/// The seeds recorded against a workload name, in the order written.
///
/// The name is the workload's `name()`, so a failure report names its own
/// corpus key. An unknown name has no seeds — a workload with no history yet.
pub fn regression_seeds(workload: &str) -> impl Iterator<Item = u64> {
    index()
        .get(workload)
        .map(Vec::as_slice)
        .unwrap_or(&[])
        .iter()
        .copied()
}

fn index() -> &'static BTreeMap<String, Vec<u64>> {
    static INDEX: OnceLock<BTreeMap<String, Vec<u64>>> = OnceLock::new();
    INDEX.get_or_init(|| parse(CORPUS))
}

/// Parse the corpus. A malformed line is a hard error rather than a skip: a typo
/// that silently dropped a regression case would defeat the file's purpose.
fn parse(text: &str) -> BTreeMap<String, Vec<u64>> {
    let mut index: BTreeMap<String, Vec<u64>> = BTreeMap::new();
    for (number, raw) in text.lines().enumerate() {
        let line = raw.split('#').next().unwrap_or("").trim();
        if line.is_empty() {
            continue;
        }
        let mut fields = line.split_whitespace();
        let (Some(workload), Some(seed)) = (fields.next(), fields.next()) else {
            panic!(
                "corpus.txt line {}: expected `<workload> <seed>`",
                number + 1
            );
        };
        let seed: u64 = seed
            .parse()
            .unwrap_or_else(|_| panic!("corpus.txt line {}: `{seed}` is not a seed", number + 1));
        index.entry(workload.to_string()).or_default().push(seed);
    }
    index
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_entries_comments_and_blank_lines() {
        let index = parse(
            "# a comment\n\
             \n\
             singleton-chaos/leader 900006  # split brain after heal\n\
             singleton-chaos/leader 12\n\
             ws/cluster 7\n",
        );
        assert_eq!(index["singleton-chaos/leader"], vec![900006, 12]);
        assert_eq!(index["ws/cluster"], vec![7]);
        assert_eq!(index.len(), 2);
    }

    #[test]
    fn unknown_workload_has_no_seeds() {
        assert_eq!(regression_seeds("never-recorded").count(), 0);
    }

    #[test]
    fn the_committed_corpus_parses() {
        // Guards the checked-in file itself: a typo fails here rather than
        // inside whichever swarm happens to run first.
        let _ = index();
    }

    #[test]
    #[should_panic(expected = "is not a seed")]
    fn a_malformed_seed_is_an_error() {
        parse("singleton-chaos/leader not-a-number\n");
    }
}
