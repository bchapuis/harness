//! Materialization between a durable `Fs` grain and a real on-disk workspace dir.
//!
//! [`DurableWorkspaces`](crate::DurableWorkspaces) routes the typed file tools
//! straight into the grain, so it never needs a real directory and offers no shell.
//! A container bind-mount or a microVM tar stream, by contrast, needs a *real*
//! directory the guest can see. This module is the bridge: [`hydrate`] rebuilds that
//! directory from the grain when a session opens, and [`sync_back`] folds the
//! directory's durable subtree into the grain when it releases — so the grain (its
//! journaled metadata and content-addressed blobs) stays the durable source of truth
//! across hibernation and migration while a shell runs against ordinary files.
//!
//! The durable/ephemeral split is the same one [`DurabilityRules`] draws for
//! `DurableWorkspaces`: excluded (regenerable) trees like `target` or `node_modules`
//! are never synced into the grain. They live only on the local dir for the
//! activation; a rebuild on the next machine regenerates them.
//!
//! **v1 bound.** Both directions are eager and capped at [`MAX_DURABLE`], mirroring
//! Firecracker's tar cap. Lazy hydration (fault in blocks on first access) and
//! streamed snapshots are the follow-up for large workspaces (research note
//! `durable-sqlite-and-filesystem.md` §4.2/§6.1).

use std::time::Duration;

use actor_core::BoxFuture;
use cap_std::fs::Dir;
use granary::GrainRef;
use granary::GranarySystem;
use granary::fs::Fs;
use granary::fs::ListDir;
use granary::fs::ReadFile;
use granary::fs::Remove;
use granary::fs::WriteFile;

use crate::durable::DurabilityRules;

/// Deadline for each grain call made while materializing. Generous on purpose:
/// the grain's client-side redirect waits out a shard election or a first-ever
/// activation up to this bound (granary §5.4), and `hydrate` runs at sandbox open
/// — on a cold cluster the very first `Fs` activation must bootstrap its quorum
/// journal, which can take longer than the 5 s default `ask` deadline under load
/// (e.g. several container runtimes starting at once). The whole open is bounded by
/// the 300 s tool timeout, so waiting tens of seconds here is safe and avoids a
/// spurious "sandbox unavailable" on the first prompt after boot.
const GRAIN_CALL_TIMEOUT: Duration = Duration::from_secs(30);

/// Cap on the total durable bytes materialized in either direction. A v1 bound,
/// matching Firecracker's `MAX_TAR` (64 MiB): eager materialization is cheap under it
/// and loud past it, until lazy hydration lands.
pub(crate) const MAX_DURABLE: u64 = 64 << 20;

/// Why a materialization round failed. Surfaced to the caller as a `String`; the
/// provider decides whether that is a transient `Sandbox` error (hydrate) or a logged
/// durability failure (sync_back) — never a silent loss.
#[derive(Debug)]
pub(crate) enum MaterializeError {
    /// A grain `ask` failed at the transport (the grain rehydrates — transient).
    Grain(String),
    /// The grain rejected a filesystem operation.
    Fs(String),
    /// A local-directory I/O error.
    Io(String),
    /// The durable subtree exceeds [`MAX_DURABLE`].
    TooLarge(u64),
}

impl std::fmt::Display for MaterializeError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            MaterializeError::Grain(e) => write!(f, "grain: {e}"),
            MaterializeError::Fs(e) => write!(f, "filesystem grain: {e}"),
            MaterializeError::Io(e) => write!(f, "workspace io: {e}"),
            MaterializeError::TooLarge(cap) => {
                write!(f, "durable workspace exceeds the {cap}-byte cap")
            }
        }
    }
}

impl std::error::Error for MaterializeError {}

/// Charge `bytes` against the remaining budget, failing if it would overrun the cap.
fn charge(budget: &mut u64, bytes: u64) -> Result<(), MaterializeError> {
    *budget = budget
        .checked_sub(bytes)
        .ok_or(MaterializeError::TooLarge(MAX_DURABLE))?;
    Ok(())
}

/// Join a listing base (`"."` at the root) with a child name into a grain path.
fn child_path(base: &str, name: &str) -> String {
    if base == "." {
        name.to_string()
    } else {
        format!("{base}/{name}")
    }
}

/// Rebuild `dir` from the grain's durable tree (on open). Files materialize through
/// the cap-std handle, so the same confinement that guards the tools guards this write
/// (S1). The dir is assumed freshly created/empty; existing durable content is
/// overwritten, matching a clean rehydration.
pub(crate) async fn hydrate<S: GranarySystem>(
    grain: &GrainRef<Fs<S>>,
    dir: &Dir,
    rules: &DurabilityRules,
) -> Result<(), MaterializeError> {
    let mut budget = MAX_DURABLE;
    hydrate_dir(grain, dir, ".", rules, &mut budget).await
}

fn hydrate_dir<'a, S: GranarySystem>(
    grain: &'a GrainRef<Fs<S>>,
    dir: &'a Dir,
    base: &'a str,
    rules: &'a DurabilityRules,
    budget: &'a mut u64,
) -> BoxFuture<'a, Result<(), MaterializeError>> {
    Box::pin(async move {
        let entries = match grain.ask_timeout(ListDir { path: base.into() }, GRAIN_CALL_TIMEOUT).await
        {
            Ok(Ok(entries)) => entries,
            Ok(Err(e)) => return Err(MaterializeError::Fs(format!("list_dir {base}: {e:?}"))),
            Err(e) => return Err(MaterializeError::Grain(format!("list_dir {base}: {e}"))),
        };
        for entry in entries {
            let path = child_path(base, &entry.name);
            // Defensive: an excluded tree should not be in the grain, but if it is,
            // it is not the local dir's business to re-create it.
            if !rules.is_durable(&path) {
                continue;
            }
            if entry.dir {
                dir.create_dir_all(&path)
                    .map_err(|e| MaterializeError::Io(format!("mkdir {path}: {e}")))?;
                hydrate_dir(grain, dir, &path, rules, budget).await?;
            } else {
                let bytes = match grain
                    .ask_timeout(ReadFile { path: path.clone(), range: None }, GRAIN_CALL_TIMEOUT)
                    .await
                {
                    Ok(Ok(bytes)) => bytes,
                    Ok(Err(e)) => {
                        return Err(MaterializeError::Fs(format!("read_file {path}: {e:?}")));
                    }
                    Err(e) => return Err(MaterializeError::Grain(format!("read_file {path}: {e}"))),
                };
                charge(budget, bytes.len() as u64)?;
                dir.write(&path, &bytes)
                    .map_err(|e| MaterializeError::Io(format!("write {path}: {e}")))?;
            }
        }
        Ok(())
    })
}

/// Fold `dir`'s durable subtree into the grain (on release). Every durable file is
/// re-written (the diff is a full replace; content-addressed blob `put`s make
/// unchanged bytes ~free), and durable files the grain holds but the dir no longer
/// has are removed — so a delete on disk propagates. Excluded trees are skipped.
pub(crate) async fn sync_back<S: GranarySystem>(
    grain: &GrainRef<Fs<S>>,
    dir: &Dir,
    rules: &DurabilityRules,
) -> Result<(), MaterializeError> {
    let mut budget = MAX_DURABLE;
    let mut on_disk = std::collections::BTreeSet::new();
    write_dir(grain, dir, ".", rules, &mut on_disk, &mut budget).await?;
    prune(grain, ".", rules, &on_disk).await?;
    Ok(())
}

fn write_dir<'a, S: GranarySystem>(
    grain: &'a GrainRef<Fs<S>>,
    dir: &'a Dir,
    base: &'a str,
    rules: &'a DurabilityRules,
    on_disk: &'a mut std::collections::BTreeSet<String>,
    budget: &'a mut u64,
) -> BoxFuture<'a, Result<(), MaterializeError>> {
    Box::pin(async move {
        // Name-sorted for a deterministic write order (no ambient ordering reaches the
        // journal), mirroring the workspace tier's `list_dir`.
        let mut names: Vec<(String, bool, bool)> = Vec::new();
        let read = dir
            .read_dir(base)
            .map_err(|e| MaterializeError::Io(format!("read_dir {base}: {e}")))?;
        for entry in read {
            let entry = entry.map_err(|e| MaterializeError::Io(format!("read_dir {base}: {e}")))?;
            let name = entry.file_name().to_string_lossy().into_owned();
            let ft = entry
                .file_type()
                .map_err(|e| MaterializeError::Io(format!("file_type {name}: {e}")))?;
            // Hard links, sockets, devices, and symlinks are not workspace content the
            // grain models — skip them, as the Firecracker untar does.
            if ft.is_dir() {
                names.push((name, true, false));
            } else if ft.is_file() {
                names.push((name, false, true));
            }
        }
        names.sort();
        for (name, is_dir, is_file) in names {
            let path = child_path(base, &name);
            if !rules.is_durable(&path) {
                continue;
            }
            if is_dir {
                write_dir(grain, dir, &path, rules, on_disk, budget).await?;
            } else if is_file {
                let bytes = dir
                    .read(&path)
                    .map_err(|e| MaterializeError::Io(format!("read {path}: {e}")))?;
                charge(budget, bytes.len() as u64)?;
                match grain
                    .ask_timeout(WriteFile { path: path.clone(), content: bytes }, GRAIN_CALL_TIMEOUT)
                    .await
                {
                    Ok(Ok(_)) => {}
                    Ok(Err(e)) => {
                        return Err(MaterializeError::Fs(format!("write_file {path}: {e:?}")));
                    }
                    Err(e) => {
                        return Err(MaterializeError::Grain(format!("write_file {path}: {e}")));
                    }
                }
                on_disk.insert(path);
            }
        }
        Ok(())
    })
}

/// Remove durable files the grain holds that no longer exist on disk (delete
/// propagation). Walks the grain tree; an excluded path can never be in the grain, but
/// it is filtered defensively all the same.
fn prune<'a, S: GranarySystem>(
    grain: &'a GrainRef<Fs<S>>,
    base: &'a str,
    rules: &'a DurabilityRules,
    on_disk: &'a std::collections::BTreeSet<String>,
) -> BoxFuture<'a, Result<(), MaterializeError>> {
    Box::pin(async move {
        let entries = match grain.ask_timeout(ListDir { path: base.into() }, GRAIN_CALL_TIMEOUT).await
        {
            Ok(Ok(entries)) => entries,
            Ok(Err(_)) => return Ok(()),
            Err(e) => return Err(MaterializeError::Grain(format!("list_dir {base}: {e}"))),
        };
        for entry in entries {
            let path = child_path(base, &entry.name);
            if !rules.is_durable(&path) {
                continue;
            }
            if entry.dir {
                prune(grain, &path, rules, on_disk).await?;
            } else if !on_disk.contains(&path) {
                match grain
                    .ask_timeout(Remove { path: path.clone(), recursive: false }, GRAIN_CALL_TIMEOUT)
                    .await
                {
                    Ok(_) => {}
                    Err(e) => return Err(MaterializeError::Grain(format!("remove {path}: {e}"))),
                }
            }
        }
        Ok(())
    })
}
