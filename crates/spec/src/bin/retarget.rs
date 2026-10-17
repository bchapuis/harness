//! Rewrite every `§` citation in `docs/` and `crates/` after a renumbering.
//!
//! ```text
//! retarget MAP OLD_ROOT [--group a,b,c]... [--write]
//! ```
//!
//! `MAP` is a [`spec::retarget::Map`]; `OLD_ROOT` holds the documents as they
//! stood before the rewrite, at their usual paths (`git worktree add` or
//! `git archive` of the old commit). Each `--group` names documents rewritten
//! together, whose texts already cite one another in the new numbering. Without
//! `--write` it reports what it would change and touches nothing.

use std::path::PathBuf;
use std::process::ExitCode;

use spec::DOCUMENTS;
use spec::Unprefixed;
use spec::citations;
use spec::retarget::Context;
use spec::retarget::Map;
use spec::retarget::OldAnchors;
use spec::retarget::rewrite;

/// One text to rewrite, with how its citations resolve.
struct File {
    path: PathBuf,
    text: String,
    code: bool,
    bare: Vec<&'static str>,
    default: Option<&'static str>,
    fresh: Vec<&'static str>,
}

fn main() -> ExitCode {
    let mut args = std::env::args().skip(1);
    let (Some(map), Some(old_root)) = (args.next(), args.next()) else {
        eprintln!("usage: retarget MAP OLD_ROOT [--group a,b,c]... [--write]");
        return ExitCode::FAILURE;
    };
    let mut groups: Vec<Vec<&'static str>> = Vec::new();
    let mut write = false;
    while let Some(arg) = args.next() {
        match arg.as_str() {
            "--write" => write = true,
            "--group" => {
                let Some(list) = args.next() else {
                    eprintln!("--group takes a comma-separated list of document ids");
                    return ExitCode::FAILURE;
                };
                let mut group = Vec::new();
                for name in list.split(',') {
                    let Some(doc) = DOCUMENTS.iter().find(|d| d.id == name) else {
                        eprintln!("--group names {name:?}, which is not registered");
                        return ExitCode::FAILURE;
                    };
                    group.push(doc.id);
                }
                groups.push(group);
            }
            other => {
                eprintln!("unknown argument {other:?}");
                return ExitCode::FAILURE;
            }
        }
    }

    let map = match std::fs::read_to_string(&map)
        .map_err(|e| e.to_string())
        .and_then(|t| Map::parse(&t))
    {
        Ok(map) => map,
        Err(e) => {
            eprintln!("{e}");
            return ExitCode::FAILURE;
        }
    };
    let old = match OldAnchors::load(&PathBuf::from(&old_root)) {
        Ok(old) => old,
        Err(e) => {
            eprintln!("reading {old_root}: {e}");
            return ExitCode::FAILURE;
        }
    };
    let root = spec::workspace_root(env!("CARGO_MANIFEST_DIR"));

    let mut files: Vec<File> = Vec::new();
    for doc in DOCUMENTS {
        let path = root.join(doc.path);
        let Ok(text) = std::fs::read_to_string(&path) else {
            continue;
        };
        let bare: Vec<&'static str> = match doc.unprefixed {
            Unprefixed::Own => vec![doc.id],
            Unprefixed::OwnThen(others) => std::iter::once(doc.id)
                .chain(others.iter().copied())
                .collect(),
            Unprefixed::Other(others) => others.to_vec(),
        };
        let default = bare.first().copied();
        let fresh = groups
            .iter()
            .find(|g| g.contains(&doc.id))
            .cloned()
            .unwrap_or_default();
        files.push(File {
            path,
            text,
            code: false,
            bare,
            default,
            fresh,
        });
    }
    let sources = match citations::rust_sources(&root) {
        Ok(sources) => sources,
        Err(e) => {
            eprintln!("{e}");
            return ExitCode::FAILURE;
        }
    };
    for (krate, path, text) in sources {
        files.push(File {
            path,
            text,
            code: true,
            bare: krate.cites.to_vec(),
            default: krate.cites.first().copied(),
            fresh: Vec::new(),
        });
    }

    let (mut moved, mut changed, mut failed) = (0usize, 0usize, 0usize);
    for File {
        path,
        text,
        code,
        bare,
        default,
        fresh,
    } in &files
    {
        let scan = if *code {
            citations::rust_comments(text)
        } else {
            text.clone()
        };
        let ctx = Context {
            bare,
            default: *default,
            fresh,
        };
        let out = rewrite(text, &scan, ctx, &map, &old);
        let shown = path.strip_prefix(&root).unwrap_or(path).display();
        for note in &out.fallbacks {
            println!("fallback {shown}:{note}");
        }
        for note in &out.failures {
            println!("FAILED {shown}:{note}");
        }
        failed += out.failures.len();
        moved += out.moved;
        if out.text != *text {
            changed += 1;
            if write && let Err(e) = std::fs::write(path, &out.text) {
                eprintln!("writing {shown}: {e}");
                return ExitCode::FAILURE;
            }
        }
    }
    println!(
        "{moved} citations moved in {changed} files ({}); {failed} need a person",
        if write { "written" } else { "dry run" }
    );
    if failed == 0 {
        ExitCode::SUCCESS
    } else {
        ExitCode::FAILURE
    }
}
