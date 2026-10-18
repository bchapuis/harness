//! The workspace as a tar stream, through a capability handle (sandbox spec
//! §4.1, S1; feature `ws`).
//!
//! Both microVM consumers move a host-side workspace directory in and out of
//! a guest as a capped tar stream over vsock: the agent sandbox's `Native`
//! tier brackets every call with it (sandbox spec §4), and the persistent
//! machine syncs its workspace facet at boot and at capture quiescent points
//! (machine spec §4). This module owns the codec; each consumer keeps its own
//! wire protocol around it.
//!
//! Every byte crosses through a `cap_std::fs::Dir` handle, so a path outside
//! the workspace is unrepresentable, not merely rejected (S1). What does
//! **not** survive the unpack, deliberately: entry paths that are absolute or
//! carry `..`, symlinks whose target is an absolute path (they would name a
//! path outside the workspace), hard links, device or fifo nodes, and the
//! suid/sgid/sticky bits (a guest must not mint a suid host file; modes are
//! masked to `0o777` in both directions). Regular files, directories,
//! relative symlinks, and the executable bit round-trip. The pack walk is
//! deterministic (sorted entries, zero mtimes) and budgeted against
//! [`MAX_TAR`] *before* bytes accumulate (headers and all), so a workspace
//! beyond the cap fails the pack rather than sizing an unmetered host
//! allocation (the sandbox spec §4.2 stance).

use std::path::Path;

use cap_std::fs::Dir;

/// Cap on one tar stream, either direction: what a guest can make the host
/// materialize must be bounded before it sizes anything.
pub const MAX_TAR: usize = 64 * 1024 * 1024;

/// Per-entry budget charge beyond file contents: the 512-byte header, name
/// extensions, and padding. Without it a workspace of a million empty files
/// would pack half a gigabyte of headers against a zero-byte budget.
const TAR_ENTRY_OVERHEAD: usize = 1024;

/// Pack the workspace. Deterministic walk (sorted entries, zero mtimes);
/// budgeted against [`MAX_TAR`] *before* bytes accumulate: headers and all.
pub fn tar_workspace(dir: &Dir) -> Result<Vec<u8>, std::io::Error> {
    let mut builder = tar::Builder::new(Vec::new());
    let mut budget = MAX_TAR;
    append_dir(&mut builder, dir, Path::new(""), &mut budget)?;
    builder.into_inner()
}

fn charge(budget: &mut usize, cost: usize) -> Result<(), std::io::Error> {
    *budget = budget.checked_sub(cost).ok_or_else(|| {
        std::io::Error::other(format!("workspace exceeds the {MAX_TAR}-byte sync cap"))
    })?;
    Ok(())
}

fn append_dir(
    builder: &mut tar::Builder<Vec<u8>>,
    dir: &Dir,
    prefix: &Path,
    budget: &mut usize,
) -> Result<(), std::io::Error> {
    let mut names: Vec<std::ffi::OsString> = dir
        .entries()?
        .map(|entry| entry.map(|e| e.file_name()))
        .collect::<Result<_, _>>()?;
    names.sort();
    for name in names {
        let path = prefix.join(&name);
        let meta = dir.symlink_metadata(&name)?;
        let kind = meta.file_type();
        charge(budget, TAR_ENTRY_OVERHEAD)?;
        if kind.is_dir() {
            let mut header = tar::Header::new_gnu();
            header.set_entry_type(tar::EntryType::Directory);
            header.set_mode(0o755);
            header.set_size(0);
            header.set_mtime(0);
            builder.append_data(&mut header, &path, std::io::empty())?;
            append_dir(builder, &dir.open_dir(&name)?, &path, budget)?;
        } else if kind.is_symlink() {
            let target = dir.read_link(&name)?;
            let mut header = tar::Header::new_gnu();
            header.set_entry_type(tar::EntryType::Symlink);
            header.set_mode(0o777);
            header.set_size(0);
            header.set_mtime(0);
            builder.append_link(&mut header, &path, &target)?;
        } else if kind.is_file() {
            charge(budget, meta.len() as usize)?;
            let mut header = tar::Header::new_gnu();
            header.set_entry_type(tar::EntryType::Regular);
            // rwx bits only: suid/sgid/sticky are not workspace semantics in
            // either direction.
            #[cfg(unix)]
            header.set_mode(cap_std::fs::PermissionsExt::mode(&meta.permissions()) & 0o777);
            #[cfg(not(unix))]
            header.set_mode(0o644);
            header.set_size(meta.len());
            header.set_mtime(0);
            builder.append_data(&mut header, &path, dir.open(&name)?)?;
        }
        // Anything else (sockets, fifos) is not representable in a
        // workspace; skipped, as on the unpack side.
    }
    Ok(())
}

/// Restore the workspace at `ws` from a guest-produced tar, **two-phase**:
/// unpack into a sibling `<ws>.incoming` staging directory, then swap the
/// trees by rename through a sibling `<ws>.outgoing` (both on the same
/// filesystem as the workspace, so no rename fails with `EXDEV`). Any failure
/// leaves the workspace as it was (machine spec §2.2): a corrupt or truncated
/// tar never reaches it, and a swap that fails part-way is undone. That
/// matters because both consumers durably capture the workspace (the sandbox
/// after every tool call, the machine at every quiescent point), and a partial
/// directory would be captured as deletions.
pub fn restore_workspace(ws: &Path, tar: &[u8]) -> Result<(), std::io::Error> {
    let incoming = sibling(ws, ".incoming");
    let outgoing = sibling(ws, ".outgoing");
    for scratch in [&incoming, &outgoing] {
        let _ = std::fs::remove_dir_all(scratch);
        std::fs::create_dir_all(scratch)?;
    }
    let staged = Dir::open_ambient_dir(&incoming, cap_std::ambient_authority())?;
    untar_workspace(&staged, tar)?;
    let aside = Dir::open_ambient_dir(&outgoing, cap_std::ambient_authority())?;
    let ws_dir = Dir::open_ambient_dir(ws, cap_std::ambient_authority())?;
    replace_workspace(&ws_dir, &staged, &aside, &mut |from, name, to| {
        from.rename(name, to, name)
    })?;
    let _ = std::fs::remove_dir_all(&incoming);
    let _ = std::fs::remove_dir_all(&outgoing);
    Ok(())
}

/// `ws` with `suffix` appended to its last component: a sibling directory.
fn sibling(ws: &Path, suffix: &str) -> std::path::PathBuf {
    let mut name = ws.as_os_str().to_owned();
    name.push(suffix);
    std::path::PathBuf::from(name)
}

/// Remove every child of the workspace, leaving the directory itself in
/// place (it may be a mount point that must survive). The clear half of the
/// replace-never-merge discipline.
fn clear_workspace(dir: &Dir) -> Result<(), std::io::Error> {
    for entry in dir.entries()? {
        let entry = entry?;
        if entry.file_type()?.is_dir() {
            dir.remove_dir_all(entry.file_name())?;
        } else {
            dir.remove_file(entry.file_name())?;
        }
    }
    Ok(())
}

/// Move one entry `name` from one directory to another, keeping its name.
type Rename<'a> = dyn FnMut(&Dir, &std::ffi::OsStr, &Dir) -> Result<(), std::io::Error> + 'a;

/// Replace `ws`'s contents with `staged`'s, or leave `ws` as it was: the
/// commit half of [`restore_workspace`]'s two-phase unpack. The workspace
/// directory itself stays in place (a sandbox bind-mounts it), so the swap
/// moves entries rather than the tree: the old ones into `aside`, then the new
/// ones in. A rename that fails sends every moved entry back where it came
/// from. Undoing is renames too and could fail in turn; that double fault
/// reports the first error, and the facet's rehydration is the backstop, the
/// directory being working state rather than the durable copy.
fn replace_workspace(
    ws: &Dir,
    staged: &Dir,
    aside: &Dir,
    rename: &mut Rename<'_>,
) -> Result<(), std::io::Error> {
    if let Err(e) = move_all(ws, aside, rename) {
        let _ = move_all(aside, ws, rename);
        return Err(e);
    }
    if let Err(e) = move_all(staged, ws, rename) {
        let _ = move_all(ws, staged, rename);
        let _ = move_all(aside, ws, rename);
        return Err(e);
    }
    Ok(())
}

/// Move every entry of `from` into `to`. The names are read before the first
/// rename, so the directory is not mutated while it is being listed.
fn move_all(from: &Dir, to: &Dir, rename: &mut Rename<'_>) -> Result<(), std::io::Error> {
    let names = from
        .entries()?
        .map(|entry| entry.map(|e| e.file_name()))
        .collect::<Result<Vec<_>, _>>()?;
    for name in names {
        rename(from, &name, to)?;
    }
    Ok(())
}

/// Unpack a guest-produced tar into `dir` (the staging half of
/// [`restore_workspace`]). Every write goes through the handle: an absolute
/// or `..`-bearing entry path is skipped here and unrepresentable below (S1,
/// twice over).
fn untar_workspace(dir: &Dir, bytes: &[u8]) -> Result<(), std::io::Error> {
    clear_workspace(dir)?;
    let mut archive = tar::Archive::new(bytes);
    for entry in archive.entries()? {
        let mut entry = entry?;
        let path = entry.path()?.into_owned();
        if path.as_os_str().is_empty()
            || !path
                .components()
                .all(|c| matches!(c, std::path::Component::Normal(_)))
        {
            continue;
        }
        match entry.header().entry_type() {
            tar::EntryType::Directory => dir.create_dir_all(&path)?,
            tar::EntryType::Regular => {
                if let Some(parent) = path.parent()
                    && !parent.as_os_str().is_empty()
                {
                    dir.create_dir_all(parent)?;
                }
                let mut file = dir.create(&path)?;
                std::io::copy(&mut entry, &mut file)?;
                // rwx bits only: a guest must not mint a suid host file.
                #[cfg(unix)]
                if let Ok(mode) = entry.header().mode() {
                    let _ = dir.set_permissions(
                        &path,
                        <cap_std::fs::Permissions as cap_std::fs::PermissionsExt>::from_mode(
                            mode & 0o777,
                        ),
                    );
                }
            }
            tar::EntryType::Symlink => {
                if let Some(target) = entry.link_name()?
                    && !target.is_absolute()
                {
                    // An absolute target names a path outside the workspace:
                    // dropped (module docs). A relative one is created;
                    // whether it may be *followed* is decided at every open,
                    // by the handle (S1).
                    let _ = dir.symlink(&target, &path);
                }
            }
            // Hard links, devices, fifos: not representable (module docs).
            _ => {}
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use cap_std::ambient_authority;
    use cap_std::fs::Dir;

    use super::*;

    fn open(dir: &tempfile::TempDir) -> Dir {
        Dir::open_ambient_dir(dir.path(), ambient_authority()).expect("open tempdir")
    }

    /// Same tree, two packs: byte-identical (sorted walk, zero mtimes).
    #[test]
    fn pack_is_deterministic() {
        let tmp = tempfile::tempdir().expect("tempdir");
        let dir = open(&tmp);
        dir.create_dir("sub").expect("mkdir");
        dir.write("sub/b.txt", b"bee").expect("write");
        dir.write("a.txt", b"ay").expect("write");
        let one = tar_workspace(&dir).expect("pack");
        let two = tar_workspace(&dir).expect("pack again");
        assert_eq!(one, two);
    }

    /// Files, directories, and relative symlinks round-trip into a second
    /// workspace; content and the executable bit survive.
    #[test]
    fn round_trips_through_a_second_workspace() {
        let src_tmp = tempfile::tempdir().expect("tempdir");
        let src = open(&src_tmp);
        src.create_dir("d").expect("mkdir");
        src.write("d/file.txt", b"contents").expect("write");
        #[cfg(unix)]
        src.symlink("d/file.txt", "link").expect("symlink");
        let tar = tar_workspace(&src).expect("pack");

        let dst_tmp = tempfile::tempdir().expect("tempdir");
        let dst = open(&dst_tmp);
        dst.write("stale.txt", b"gone").expect("write");
        untar_workspace(&dst, &tar).expect("unpack");

        assert_eq!(dst.read("d/file.txt").expect("read"), b"contents");
        assert!(!dst.exists("stale.txt"), "unpack replaces, never merges");
        #[cfg(unix)]
        assert_eq!(dst.read("link").expect("follow link"), b"contents");
    }

    /// A workspace past the budget fails the pack: driven through the real
    /// walk with a small starting budget rather than a multi-GiB tree.
    #[test]
    fn pack_charges_the_budget() {
        let tmp = tempfile::tempdir().expect("tempdir");
        let dir = open(&tmp);
        dir.write("a.bin", [0u8; 512]).expect("write");
        dir.write("b.bin", [0u8; 512]).expect("write");
        // Two entries cost 2 * TAR_ENTRY_OVERHEAD + 1024 content bytes; a
        // budget covering one entry but not both must refuse mid-walk.
        let mut builder = tar::Builder::new(Vec::new());
        let mut budget = TAR_ENTRY_OVERHEAD + 512 + TAR_ENTRY_OVERHEAD;
        assert!(
            append_dir(&mut builder, &dir, Path::new(""), &mut budget).is_err(),
            "the walk must charge the budget before bytes accumulate"
        );
    }

    /// The staged two-phase replace: a swap delivers the staged tree whole,
    /// and the cleared workspace directory itself survives (mount-point
    /// semantics).
    #[test]
    fn replace_workspace_swaps_whole_trees() {
        let ws_tmp = tempfile::tempdir().expect("tempdir");
        let staged_tmp = tempfile::tempdir().expect("tempdir");
        let ws = open(&ws_tmp);
        let staged = open(&staged_tmp);
        ws.write("stale.txt", b"old").expect("write");
        staged.create_dir("d").expect("mkdir");
        staged.write("d/new.txt", b"new").expect("write");
        let aside_tmp = tempfile::tempdir().expect("tempdir");
        replace_workspace(&ws, &staged, &open(&aside_tmp), &mut |from, name, to| {
            from.rename(name, to, name)
        })
        .expect("replace");
        assert!(!ws.exists("stale.txt"));
        assert_eq!(ws.read("d/new.txt").expect("read"), b"new");
        assert!(ws_tmp.path().is_dir(), "the workspace directory survives");
    }

    /// A swap that fails at any one of its renames leaves the workspace as it
    /// was (machine spec §2.2): every moved entry goes back, old and new alike.
    #[test]
    fn a_swap_failing_at_any_rename_leaves_the_workspace_as_it_was() {
        let build = |old: &Dir, new: &Dir| {
            old.write("a.txt", b"old a").expect("write");
            old.create_dir("d").expect("mkdir");
            old.write("d/b.txt", b"old b").expect("write");
            new.write("a.txt", b"new a").expect("write");
            new.write("c.txt", b"new c").expect("write");
        };
        // Two old entries move aside and two new ones move in.
        for fail_at in 0..4 {
            let (ws_tmp, staged_tmp, aside_tmp) = (
                tempfile::tempdir().expect("tempdir"),
                tempfile::tempdir().expect("tempdir"),
                tempfile::tempdir().expect("tempdir"),
            );
            let (ws, staged, aside) = (open(&ws_tmp), open(&staged_tmp), open(&aside_tmp));
            build(&ws, &staged);
            let before = tar_workspace(&ws).expect("pack");
            let mut calls = 0;
            let result = replace_workspace(&ws, &staged, &aside, &mut |from, name, to| {
                calls += 1;
                if calls == fail_at + 1 {
                    return Err(std::io::Error::other("injected"));
                }
                from.rename(name, to, name)
            });
            assert!(result.is_err(), "rename {fail_at} was made to fail");
            assert_eq!(
                tar_workspace(&ws).expect("pack"),
                before,
                "a swap failing at rename {fail_at} left the workspace changed"
            );
            assert_eq!(
                aside.entries().expect("list").count(),
                0,
                "nothing left aside"
            );
        }
    }

    /// Escape-shaped entries are dropped at the boundary: absolute paths,
    /// `..` components, absolute symlink targets.
    #[test]
    fn unpack_drops_escapes() {
        let mut builder = tar::Builder::new(Vec::new());
        let add_file = |builder: &mut tar::Builder<Vec<u8>>, path: &str| {
            // The tar crate refuses to *build* escape-shaped paths, so write
            // the GNU header's name bytes directly, as an attacker would.
            let mut header = tar::Header::new_gnu();
            header.set_entry_type(tar::EntryType::Regular);
            header.set_mode(0o644);
            header.set_size(2);
            header.set_mtime(0);
            {
                let name = &mut header.as_gnu_mut().expect("gnu header").name;
                name[..path.len()].copy_from_slice(path.as_bytes());
            }
            header.set_cksum();
            builder.append(&header, &b"xx"[..]).expect("append");
        };
        add_file(&mut builder, "ok.txt");
        add_file(&mut builder, "../escape.txt");
        let mut header = tar::Header::new_gnu();
        header.set_entry_type(tar::EntryType::Symlink);
        header.set_mode(0o777);
        header.set_size(0);
        header.set_mtime(0);
        builder
            .append_link(&mut header, "abs-link", "/etc/passwd")
            .expect("append link");
        let tar = builder.into_inner().expect("finish");

        let tmp = tempfile::tempdir().expect("tempdir");
        let dir = open(&tmp);
        untar_workspace(&dir, &tar).expect("unpack");
        assert!(dir.exists("ok.txt"));
        assert!(!dir.exists("abs-link"), "absolute symlink target dropped");
        assert!(
            !tmp.path()
                .parent()
                .expect("parent")
                .join("escape.txt")
                .exists(),
            "`..` entry dropped"
        );
    }

    /// suid/sgid/sticky are masked in the unpack direction.
    #[cfg(unix)]
    #[test]
    fn unpack_masks_setuid() {
        let mut builder = tar::Builder::new(Vec::new());
        let mut header = tar::Header::new_gnu();
        header.set_entry_type(tar::EntryType::Regular);
        header.set_mode(0o4755);
        header.set_size(2);
        header.set_mtime(0);
        builder
            .append_data(&mut header, "suid.bin", &b"xx"[..])
            .expect("append");
        let tar = builder.into_inner().expect("finish");

        let tmp = tempfile::tempdir().expect("tempdir");
        let dir = open(&tmp);
        untar_workspace(&dir, &tar).expect("unpack");
        let mode = cap_std::fs::PermissionsExt::mode(
            &dir.metadata("suid.bin").expect("meta").permissions(),
        );
        assert_eq!(mode & 0o7000, 0, "suid/sgid/sticky masked");
    }
}
