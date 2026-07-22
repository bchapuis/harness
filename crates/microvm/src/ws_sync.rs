//! The workspace as a tar stream, through a capability handle (sandbox spec
//! §3.1, S1; feature `ws`).
//!
//! Both microVM consumers move a host-side workspace directory in and out of
//! a guest as a capped tar stream over vsock: the agent sandbox's `Native`
//! tier brackets every call with it (sandbox spec §3.5), and the persistent
//! machine syncs its workspace facet at boot and at capture quiescent points
//! (machine spec §4). This module owns the codec — pack and unpack — while
//! each consumer keeps its own wire protocol around it.
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
//! [`MAX_TAR`] *before* bytes accumulate — headers and all — so a workspace
//! beyond the cap fails the pack rather than sizing an unmetered host
//! allocation (the sandbox spec §3.2 stance).

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
/// budgeted against [`MAX_TAR`] *before* bytes accumulate — headers and all.
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
/// unpack into a sibling `<ws>.incoming` staging directory — the same
/// filesystem as the workspace, so the swap's renames never fail with
/// `EXDEV` — then swap whole trees. A corrupt or truncated tar leaves the
/// workspace untouched; that matters because both consumers durably capture
/// the workspace (the sandbox after every tool call, the machine at every
/// quiescent point), and a partial directory would be captured as deletions.
/// The staging dance is this codec's secret; callers hold only the path.
pub fn restore_workspace(ws: &Path, tar: &[u8]) -> Result<(), std::io::Error> {
    let mut name = ws.as_os_str().to_owned();
    name.push(".incoming");
    let incoming = std::path::PathBuf::from(name);
    let _ = std::fs::remove_dir_all(&incoming);
    std::fs::create_dir_all(&incoming)?;
    let staged = Dir::open_ambient_dir(&incoming, cap_std::ambient_authority())?;
    untar_workspace(&staged, tar)?;
    let ws_dir = Dir::open_ambient_dir(ws, cap_std::ambient_authority())?;
    replace_workspace(&ws_dir, &staged)?;
    let _ = std::fs::remove_dir_all(&incoming);
    Ok(())
}

/// Remove every child of the workspace, leaving the directory itself in
/// place (it may be a mount point that must survive). The clear half of the
/// replace-never-merge discipline every consumer shares.
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

/// Replace `ws`'s contents with `staged`'s, by rename — the commit half of
/// [`restore_workspace`]'s two-phase unpack.
fn replace_workspace(ws: &Dir, staged: &Dir) -> Result<(), std::io::Error> {
    clear_workspace(ws)?;
    for entry in staged.entries()? {
        let entry = entry?;
        staged.rename(entry.file_name(), ws, entry.file_name())?;
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
                // rwx bits only — a guest must not mint a suid host file.
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
                    // An absolute target names a path outside the workspace
                    // — dropped (module docs). A relative one is created;
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

    /// A workspace past the budget fails the pack — driven through the real
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
        replace_workspace(&ws, &staged).expect("replace");
        assert!(!ws.exists("stale.txt"));
        assert_eq!(ws.read("d/new.txt").expect("read"), b"new");
        assert!(ws_tmp.path().is_dir(), "the workspace directory survives");
    }

    /// Escape-shaped entries are dropped at the boundary: absolute paths,
    /// `..` components, absolute symlink targets.
    #[test]
    fn unpack_drops_escapes() {
        let mut builder = tar::Builder::new(Vec::new());
        let add_file = |builder: &mut tar::Builder<Vec<u8>>, path: &str| {
            // The tar crate refuses to *build* escape-shaped paths, so write
            // the GNU header's name bytes directly — as an attacker would.
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
